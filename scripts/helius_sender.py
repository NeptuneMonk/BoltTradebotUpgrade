"""
Helius Sender client — ultra-low-latency tx submission with dual routing
(validators + Jito).

Critical notes from the Helius docs (https://www.helius.dev/docs/sending-transactions/sender):
  - Free on every plan, no API credits consumed.
  - REQUIREMENT 1: `skipPreflight: true` is mandatory.
  - REQUIREMENT 2: `maxRetries: 0` is mandatory (Sender handles retry routing).
  - REQUIREMENT 3: every tx MUST include a SOL transfer to a designated tip
    account inside the same transaction (a separate `SystemProgram.transfer`
    instruction). Without it, Sender rejects.
  - REQUIREMENT 4: every tx MUST include a priority fee
    (`ComputeBudgetProgram.setComputeUnitPrice`).

Two routing modes:
  - "dual"   → both validators AND Jito; 0.0002 SOL minimum tip (~$0.04).
               Use for emergency/force exits where landing > fee cost.
  - "swqos"  → SWQOS-only (Jito infra path); 0.000005 SOL minimum tip
               (~$0.001). Cheap enough for normal-flow micro-stake sells.

We use the **global HTTPS endpoint** by default (auto-routes to nearest
location); operator can override with `HELIUS_SENDER_ENDPOINT` env var to
co-locate a regional HTTP endpoint (slc / ewr / lon / fra / ams / sg / tyo).

Confirmation polling reuses our existing RPC + the `getTransaction.meta.err`
check so we still catch instruction-level errors (Custom:XXXX) that the
signature-status view is blind to.
"""
import asyncio
import base64
import logging
import os
import random
from typing import Literal

import httpx
from solana_client import rpc_call
from solders.hash import Hash
from solders.instruction import AccountMeta, Instruction
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction
from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price

logger = logging.getLogger("helius_sender")

# Allow operator to pin a regional endpoint via env (e.g. ewr for us-east infra).
# Default: global HTTPS endpoint, auto-routes to nearest pop.
SENDER_BASE = os.environ.get(
    "HELIUS_SENDER_ENDPOINT", "https://sender.helius-rpc.com"
).rstrip("/")

# Designated tip accounts (mainnet-beta). Rotate randomly per tx to balance
# load and avoid sequencing dependencies on any single account.
TIP_ACCOUNTS = [
    "4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE",
    "D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ",
    "9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta",
    "5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn",
    "2nyhqdwKcJZR2vcqCyrYsaPVdAnFoJjiksCXJ7hfEYgD",
    "2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ",
    "wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF",
    "3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT",
    "4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey",
    "4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or",
]

# Tip floors per Sender docs
TIP_LAMPORTS_DUAL = 200_000   # 0.0002 SOL — dual routing (validators + Jito)
TIP_LAMPORTS_SWQOS = 5_000    # 0.000005 SOL — SWQOS-only routing

SYSTEM_PROGRAM_ID = Pubkey.from_string("11111111111111111111111111111111")


def _build_tip_transfer_ix(payer: Pubkey, lamports: int) -> Instruction:
    """SystemProgram.transfer encoded manually so we don't pull in another
    dep. Layout: 4-byte LE discriminator (2 = Transfer) + 8-byte LE lamports."""
    dest = Pubkey.from_string(random.choice(TIP_ACCOUNTS))
    data = b"\x02\x00\x00\x00" + int(lamports).to_bytes(8, "little")
    return Instruction(
        program_id=SYSTEM_PROGRAM_ID,
        data=data,
        accounts=[
            AccountMeta(pubkey=payer, is_signer=True, is_writable=True),
            AccountMeta(pubkey=dest, is_signer=False, is_writable=True),
        ],
    )


async def _poll_confirmation(sig: str, timeout_s: float) -> None:
    """Same confirmation pattern as pumpfun.send_versioned_tx but standalone
    so we don't import-cycle. Polls getSignatureStatuses, then verifies via
    getTransaction.meta.err for instruction-level failures.

    Raises RuntimeError if landed-but-failed or timed out.
    """
    deadline = asyncio.get_event_loop().time() + timeout_s
    poll = 0.5
    last_err = None
    while asyncio.get_event_loop().time() < deadline:
        try:
            st_res = await rpc_call(
                "getSignatureStatuses",
                [[sig], {"searchTransactionHistory": False}],
            )
            value = ((st_res.get("result") or {}).get("value") or [None])[0]
            if value:
                err = value.get("err")
                conf = value.get("confirmationStatus") or ""
                if err is not None:
                    last_err = err
                    break
                if conf in ("confirmed", "finalized"):
                    # Verify instruction-level success via getTransaction.meta.err
                    try:
                        tx_res = await rpc_call(
                            "getTransaction",
                            [sig, {
                                "encoding": "json",
                                "commitment": "confirmed",
                                "maxSupportedTransactionVersion": 0,
                            }],
                        )
                        tx_obj = tx_res.get("result")
                        if tx_obj:
                            meta_err = ((tx_obj.get("meta") or {}).get("err"))
                            if meta_err is not None:
                                last_err = meta_err
                                break
                    except Exception:
                        pass
                    return  # confirmed AND no instruction error
        except Exception as e:
            last_err = e
        await asyncio.sleep(poll)
        poll = min(poll * 1.3, 1.5)

    if last_err is not None:
        raise RuntimeError(f"sender tx {sig[:16]}… landed but failed: {last_err}")
    raise RuntimeError(
        f"sender tx {sig[:16]}… not confirmed in {timeout_s}s (blockhash likely expired)"
    )


async def send_via_sender(
    keypair,
    instructions: list,
    *,
    priority_fee_microlamports: int = 1_000_000,
    compute_unit_limit: int = 400_000,
    mode: Literal["dual", "swqos"] = "swqos",
    confirm_timeout_s: float = 60.0,
) -> str:
    """Build, sign, and submit a versioned tx through Helius Sender.

    Drop-in replacement for `pumpfun.send_versioned_tx` for any sell where
    landing reliability matters more than the tip cost. Returns the
    transaction signature on confirmation, raises on failure/timeout.

    Mode selection:
      - "dual"  → 0.0002 SOL tip, validators + Jito dual routing. Best for
                  emergency / force-recovery sells where missing the
                  block = real losses.
      - "swqos" → 0.000005 SOL tip, SWQOS-only. Cheap enough for general
                  exits on micro-stake positions ($0.50 - $1).
    """
    tip_lamports = TIP_LAMPORTS_DUAL if mode == "dual" else TIP_LAMPORTS_SWQOS
    endpoint = f"{SENDER_BASE}/fast" if mode == "dual" else f"{SENDER_BASE}/fast?swqos_only=true"

    payer = keypair.pubkey()
    tip_ix = _build_tip_transfer_ix(payer, tip_lamports)

    # Compose: CU limit + CU price + user instructions + tip transfer.
    # Tip can be anywhere in the tx; we append for clarity.
    ixs = [
        set_compute_unit_limit(compute_unit_limit),
        set_compute_unit_price(priority_fee_microlamports),
        *instructions,
        tip_ix,
    ]

    # 'confirmed' commitment gives ~60s landing window vs 'finalized' ~30s
    bh_res = await rpc_call("getLatestBlockhash", [{"commitment": "confirmed"}])
    blockhash_str = bh_res["result"]["value"]["blockhash"]
    blockhash = Hash.from_string(blockhash_str)

    msg = MessageV0.try_compile(
        payer=payer,
        instructions=ixs,
        address_lookup_table_accounts=[],
        recent_blockhash=blockhash,
    )
    tx = VersionedTransaction(msg, [keypair])
    raw_b64 = base64.b64encode(bytes(tx)).decode("utf-8")

    payload = {
        "jsonrpc": "2.0",
        "id": "sender",
        "method": "sendTransaction",
        "params": [
            raw_b64,
            {"encoding": "base64", "skipPreflight": True, "maxRetries": 0},
        ],
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            r = await client.post(endpoint, json=payload)
            r.raise_for_status()
            j = r.json()
        except Exception as e:
            raise RuntimeError(f"sender post failed ({endpoint}): {e}")

    if "error" in j and j["error"]:
        raise RuntimeError(f"sender sendTransaction error: {j['error']}")
    sig = j.get("result")
    if not sig:
        raise RuntimeError(f"sender returned no signature: {j!r}")

    logger.info(
        f"sender submitted ({mode}, tip={tip_lamports/1e9:.6f} SOL, "
        f"prio={priority_fee_microlamports} µL): {sig[:16]}…"
    )
    await _poll_confirmation(sig, confirm_timeout_s)
    return sig
