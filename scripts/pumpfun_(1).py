"""
Pump.fun on-chain integration — aligned with the 2026-04-28 program upgrade.

Key changes from the legacy layout (which was silently failing every tx with
`IncorrectProgramId`):
  - Buy IX:  18 accounts (was 12). Adds creator_vault, global/user volume
             accumulators, fee_config, fee_program, bonding_curve_v2, and a
             writable breaking_fee_recipient (picked from 8 fixed pubkeys).
  - Sell IX: 15 accounts (was 12). Same new accounts (no volume accumulators
             unless the coin is a cashback coin — we skip cashback for now).
  - Instruction args: appended OptionBool track_volume = Some(true) → 2 bytes
                      `[1, 1]` at the tail of buy/sell data.

Reference: github.com/chainstacklabs/pumpfun-bonkfun-bot (2026-04-27 commit
22a0c23, the maintained reference that aligns with the live program).
"""
import os
import random
import struct
import base64
import httpx
from solders.pubkey import Pubkey
from solders.instruction import Instruction, AccountMeta
from solders.transaction import VersionedTransaction
from solders.message import MessageV0
from solders.system_program import ID as SYSTEM_PROGRAM_ID
from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price

from solana_client import rpc_call, LAMPORTS_PER_SOL

# ---------- Program constants ----------
PUMP_PROGRAM_ID = Pubkey.from_string(os.environ["PUMP_PROGRAM_ID"])
GLOBAL_PDA = Pubkey.from_string(os.environ["PUMP_GLOBAL"])
FEE_RECIPIENT = Pubkey.from_string(os.environ["PUMP_FEE_RECIPIENT"])
EVENT_AUTHORITY = Pubkey.from_string(os.environ["PUMP_EVENT_AUTHORITY"])

# Mayhem-mode fee recipient: used for Token-2022 ("mayhem") coins introduced
# with the 2026-04-28 upgrade. Hardcoded per chainstack reference; cross-check
# at Global account offset 483 if Pump.fun rotates it.
MAYHEM_FEE_RECIPIENT = Pubkey.from_string(
    "GesfTA3X2arioaHp8bbKdjG9vJtskViWACZoYvxp4twS"
)

# 2026-04-28 upgrade: fee handling now routes through a separate program +
# config PDA. These are fixed across all coins.
FEE_PROGRAM = Pubkey.from_string("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ")

# 2026-04-28 upgrade: each buy/sell must append ONE of these 8 writable
# "breaking-upgrade fee recipients". Pump.fun's docs recommend picking at
# random per tx to spread load. (BREAKING_FEE_RECIPIENT.md)
BREAKING_FEE_RECIPIENTS = [
    Pubkey.from_string("5YxQFdt3Tr9zJLvkFccqXVUwhdTWJQc1fFg2YPbxvxeD"),
    Pubkey.from_string("9M4giFFMxmFGXtc3feFzRai56WbBqehoSeRE5GK7gf7"),
    Pubkey.from_string("GXPFM2caqTtQYC2cJ5yJRi9VDkpsYZXzYdwYpGnLmtDL"),
    Pubkey.from_string("3BpXnfJaUTiwXnJNe7Ej1rcbzqTTQUvLShZaWazebsVR"),
    Pubkey.from_string("5cjcW9wExnJJiqgLjq7DEG75Pm6JBgE1hNv4B2vHXUW6"),
    Pubkey.from_string("EHAAiTxcdDwQ3U4bU6YcMsQGaekdzLS3B5SmYo46kJtL"),
    Pubkey.from_string("5eHhjP8JaYkz83CWwvGU2uMUXefd3AazWGx4gpcuEEYD"),
    Pubkey.from_string("A7hAgCzFw14fejgCp387JUJRMNyz4j89JKnhtKU8piqW"),
]


def _pick_breaking_fee_recipient() -> Pubkey:
    """Pick a breaking-fee recipient with weighted-by-health selection.

    Each tx attempts to record success/failure via `record_breaking_fee_result`.
    Recipients with high recent failure rates are deprioritised but never
    fully excluded — Pump.fun's program will reject txs that consistently
    use one recipient, so we still spread load.

    Selection: 70% weight by inverse fail-rate, 30% uniform random (fallback
    for cold recipients with no data yet).
    """
    if not _RECIPIENT_STATS:
        return random.choice(BREAKING_FEE_RECIPIENTS)
    # 30% pure random for exploration
    if random.random() < 0.3:
        return random.choice(BREAKING_FEE_RECIPIENTS)
    # 70%: weight by (1 - fail_rate). Add 0.1 floor so even bad ones get picked.
    weights = []
    for r in BREAKING_FEE_RECIPIENTS:
        stats = _RECIPIENT_STATS.get(str(r), {"ok": 0, "fail": 0})
        total = stats["ok"] + stats["fail"]
        if total < 3:  # too little data → treat as healthy
            weights.append(1.0)
        else:
            fail_rate = stats["fail"] / total
            weights.append(max(0.1, 1.0 - fail_rate))
    return random.choices(BREAKING_FEE_RECIPIENTS, weights=weights, k=1)[0]


# Per-recipient health stats: {pubkey_str: {"ok": int, "fail": int}}
# Decays over time so old failures don't poison forever. Reset every 200 txs.
_RECIPIENT_STATS: dict[str, dict[str, int]] = {}
_RECIPIENT_DECAY_THRESHOLD = 200


def record_breaking_fee_result(recipient: Pubkey, success: bool) -> None:
    """Called by `bot.py` after a buy/sell tx confirms or fails. Tracks per-
    recipient health so `_pick_breaking_fee_recipient` can avoid sick ones."""
    key = str(recipient)
    stats = _RECIPIENT_STATS.setdefault(key, {"ok": 0, "fail": 0})
    if success:
        stats["ok"] += 1
    else:
        stats["fail"] += 1
    # Decay: when any recipient accumulates >threshold attempts, halve all
    # counters so the window slides instead of growing unbounded.
    total = stats["ok"] + stats["fail"]
    if total > _RECIPIENT_DECAY_THRESHOLD:
        for k in _RECIPIENT_STATS:
            _RECIPIENT_STATS[k]["ok"] //= 2
            _RECIPIENT_STATS[k]["fail"] //= 2


def get_recipient_health_snapshot() -> dict[str, dict]:
    """Diagnostic — returns current recipient health for debugging / UI."""
    out = {}
    for r in BREAKING_FEE_RECIPIENTS:
        key = str(r)
        s = _RECIPIENT_STATS.get(key, {"ok": 0, "fail": 0})
        total = s["ok"] + s["fail"]
        fail_rate = s["fail"] / total if total else 0.0
        out[key[:8] + "…"] = {
            "ok": s["ok"], "fail": s["fail"],
            "fail_rate": round(fail_rate, 3),
        }
    return out


def record_ix_outcome(ix: Instruction, success: bool) -> None:
    """Extract the breaking-fee recipient (always the LAST AccountMeta in our
    buy/sell IX layout) from a Pump.fun instruction and record the outcome.
    Safe to call with any ix — silently no-ops if structure doesn't match."""
    try:
        if not ix.accounts:
            return
        # Buy IX: breaking_fee is account[17]. Sell IX: account[14 or 15].
        # Either way, it's the LAST account in our builders.
        recipient = ix.accounts[-1].pubkey
        if recipient in BREAKING_FEE_RECIPIENTS:
            record_breaking_fee_result(recipient, success)
    except Exception:
        pass  # diagnostic-only, never block trading


# Authorized fee_recipients for account[1] in buy/sell ixs (post-2026-04-28).
# These were extracted by scanning recent SUCCESSFUL on-chain Pump.fun trades.
# The Global account also contains them but at non-uniform offsets mixed with
# unrelated fields, so an offset-based scan can pick up an invalid pubkey and
# trip NotAuthorized (6000). Using the observed-good list is the safe path.
AUTHORIZED_FEE_RECIPIENTS = [
    Pubkey.from_string("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV"),  # legacy
    Pubkey.from_string("7VtfL8fvgNfhz17qKRMjzQEXgbdpnHHHQRh54R9jP2RJ"),
    Pubkey.from_string("9rPYyANsfQZw3DnDmKE3YCQF5E8oD89UXoHn9JFEhJUz"),
    Pubkey.from_string("AVmoTthdrX6tKt4nDjco2D775W2YK3sDhxPcMmzUAmTY"),
    Pubkey.from_string("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM"),
    Pubkey.from_string("FWsW1xNtWscwNmKv6wVsU1iTzRN6wmmk3MjxRP5tT7hz"),
    Pubkey.from_string("G5UZAVbAf46s7cKWoyKu8kYTip9DGTpbLZ2qa9Aq69dP"),
    # TSLvdd1pWp... removed 2026-05-24 — appeared in some tx scans but the
    # program rejects it with NotAuthorized (6000). Kept the 7 confirmed-good.
]


async def pick_fee_recipient() -> Pubkey:
    return random.choice(AUTHORIZED_FEE_RECIPIENTS)


async def get_authorized_fee_recipients() -> list[Pubkey]:
    return list(AUTHORIZED_FEE_RECIPIENTS)


TOKEN_PROGRAM = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
TOKEN_2022_PROGRAM = Pubkey.from_string("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
ASSOCIATED_TOKEN_PROGRAM = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
RENT_SYSVAR = Pubkey.from_string("SysvarRent111111111111111111111111111111111")

# Anchor 8-byte discriminators (unchanged across the 2026-04-28 upgrade —
# only the instruction args and account list changed).
BUY_DISCRIMINATOR = bytes([102, 6, 61, 18, 1, 218, 235, 234])
SELL_DISCRIMINATOR = bytes([51, 230, 133, 164, 1, 127, 131, 173])
CREATE_DISCRIMINATOR = bytes([24, 30, 200, 40, 5, 28, 7, 119])

# Tail bytes for buy/sell args: OptionBool track_volume = Some(true)
TRACK_VOLUME_TAIL = bytes([1, 1])

# Pump.fun initial bonding curve constants
INITIAL_VIRTUAL_SOL_RESERVES = 30_000_000_000  # 30 SOL in lamports
INITIAL_VIRTUAL_TOKEN_RESERVES = 1_073_000_000_000_000  # 1.073B tokens × 1e6
LAUNCH_BASELINE_PRICE_SOL = (
    INITIAL_VIRTUAL_SOL_RESERVES / INITIAL_VIRTUAL_TOKEN_RESERVES / LAMPORTS_PER_SOL
)


# ---------- Token program detection ----------
async def get_mint_token_program(mint_str: str) -> Pubkey:
    """Return the token program (classic SPL or Token-2022) that owns the mint.

    Post-2026-04-28 upgrade, Pump.fun supports both. The buy/sell ix MUST pass
    the right one as the `token_program` account or the inner ATA CPI fails
    with `IncorrectProgramId`.
    """
    res = await rpc_call(
        "getAccountInfo",
        [mint_str, {"encoding": "base64", "commitment": "confirmed"}],
    )
    val = (res.get("result") or {}).get("value")
    if not val:
        return TOKEN_PROGRAM
    owner = val.get("owner") or ""
    try:
        return Pubkey.from_string(owner)
    except Exception:
        return TOKEN_PROGRAM


def derive_associated_token_for_program(owner: Pubkey, mint: Pubkey, token_program: Pubkey) -> Pubkey:
    pda, _ = Pubkey.find_program_address(
        [bytes(owner), bytes(token_program), bytes(mint)],
        ASSOCIATED_TOKEN_PROGRAM,
    )
    return pda


# ---------- PDA derivations ----------
def derive_bonding_curve(mint: Pubkey) -> Pubkey:
    pda, _ = Pubkey.find_program_address(
        [b"bonding-curve", bytes(mint)], PUMP_PROGRAM_ID
    )
    return pda


def derive_associated_token(owner: Pubkey, mint: Pubkey) -> Pubkey:
    pda, _ = Pubkey.find_program_address(
        [bytes(owner), bytes(TOKEN_PROGRAM), bytes(mint)],
        ASSOCIATED_TOKEN_PROGRAM,
    )
    return pda


def derive_creator_vault(creator: Pubkey) -> Pubkey:
    pda, _ = Pubkey.find_program_address(
        [b"creator-vault", bytes(creator)], PUMP_PROGRAM_ID
    )
    return pda


def derive_global_volume_accumulator() -> Pubkey:
    pda, _ = Pubkey.find_program_address(
        [b"global_volume_accumulator"], PUMP_PROGRAM_ID
    )
    return pda


def derive_user_volume_accumulator(user: Pubkey) -> Pubkey:
    pda, _ = Pubkey.find_program_address(
        [b"user_volume_accumulator", bytes(user)], PUMP_PROGRAM_ID
    )
    return pda


def derive_bonding_curve_v2(mint: Pubkey) -> Pubkey:
    pda, _ = Pubkey.find_program_address(
        [b"bonding-curve-v2", bytes(mint)], PUMP_PROGRAM_ID
    )
    return pda


def derive_fee_config() -> Pubkey:
    # Note: seeded under FEE_PROGRAM, NOT PUMP_PROGRAM_ID
    pda, _ = Pubkey.find_program_address(
        [b"fee_config", bytes(PUMP_PROGRAM_ID)], FEE_PROGRAM
    )
    return pda


# ---------- Bonding curve state ----------
async def fetch_bonding_curve_state(mint_str: str) -> dict | None:
    """
    Fetch the bonding curve account data.
    Layout (Anchor, post-2026-04-28):
      0..8    discriminator
      8..16   virtual_token_reserves (u64)
      16..24  virtual_sol_reserves (u64)
      24..32  real_token_reserves (u64)
      32..40  real_sol_reserves (u64)
      40..48  token_total_supply (u64)
      48      complete (bool)
      49..81  creator (Pubkey)
      81      reserved (u8)
      82      cashback_enabled (bool)   ← determines sell IX shape
      83..    extended fields
    """
    mint = Pubkey.from_string(mint_str)
    bc = derive_bonding_curve(mint)
    res = await rpc_call(
        "getAccountInfo",
        [str(bc), {"encoding": "base64", "commitment": "confirmed"}],
    )
    val = res.get("result", {}).get("value")
    if not val:
        return None
    data_b64 = val["data"][0]
    data = base64.b64decode(data_b64)
    if len(data) < 49:
        return None
    vtr, vsr, rtr, rsr, tts = struct.unpack_from("<QQQQQ", data, 8)
    complete = bool(data[48])
    # Creator stored ON the bonding curve (offset 49-81). MUST use this for the
    # creator_vault PDA — the launch metadata's "creator" can differ for
    # tokens minted via Pump.fun's deployer-as-a-service, causing the program
    # to reject the IX with ConstraintSeeds (2006).
    try:
        creator = str(Pubkey(data[49:81])) if len(data) >= 81 else None
    except Exception:
        creator = None
    is_cashback = bool(data[82]) if len(data) > 82 else False
    return {
        "virtual_token_reserves": vtr,
        "virtual_sol_reserves": vsr,
        "real_token_reserves": rtr,
        "real_sol_reserves": rsr,
        "token_total_supply": tts,
        "complete": complete,
        "creator": creator,
        "is_cashback": is_cashback,
    }


# ---------- Quote math (unchanged constant-product) ----------
def quote_buy_tokens(state: dict, sol_in_lamports: int, slippage_bps: int = 500) -> tuple[int, int]:
    vsr = state["virtual_sol_reserves"]
    vtr = state["virtual_token_reserves"]
    new_vsr = vsr + sol_in_lamports
    new_vtr = (vsr * vtr) // new_vsr
    tokens_out = vtr - new_vtr
    max_sol = sol_in_lamports + (sol_in_lamports * slippage_bps) // 10_000
    return tokens_out, max_sol


def quote_sell_sol(state: dict, tokens_in: int, slippage_bps: int = 500) -> tuple[int, int]:
    vsr = state["virtual_sol_reserves"]
    vtr = state["virtual_token_reserves"]
    new_vtr = vtr + tokens_in
    new_vsr = (vsr * vtr) // new_vtr
    sol_out = vsr - new_vsr
    min_sol = sol_out - (sol_out * slippage_bps) // 10_000
    return sol_out, max(0, min_sol)


# ---------- Instruction builders ----------
def build_create_ata_ix(payer: Pubkey, owner: Pubkey, mint: Pubkey, token_program: Pubkey = TOKEN_PROGRAM) -> Instruction:
    ata = derive_associated_token_for_program(owner, mint, token_program)
    return Instruction(
        program_id=ASSOCIATED_TOKEN_PROGRAM,
        data=bytes([1]),  # CreateIdempotent
        accounts=[
            AccountMeta(payer, True, True),
            AccountMeta(ata, False, True),
            AccountMeta(owner, False, False),
            AccountMeta(mint, False, False),
            AccountMeta(SYSTEM_PROGRAM_ID, False, False),
            AccountMeta(token_program, False, False),
        ],
    )


async def build_buy_ix(
    user: Pubkey,
    mint: Pubkey,
    amount_tokens: int,
    max_sol_cost_lamports: int,
    creator: Pubkey,
    token_program: Pubkey = TOKEN_PROGRAM,
) -> Instruction:
    """Build a Pump.fun buy instruction with the post-2026-04-28 18-account layout."""
    bonding_curve = derive_bonding_curve(mint)
    associated_bonding_curve = derive_associated_token_for_program(bonding_curve, mint, token_program)
    associated_user = derive_associated_token_for_program(user, mint, token_program)
    creator_vault = derive_creator_vault(creator)
    global_vol_acc = derive_global_volume_accumulator()
    user_vol_acc = derive_user_volume_accumulator(user)
    fee_config = derive_fee_config()
    bonding_curve_v2 = derive_bonding_curve_v2(mint)
    breaking_fee = _pick_breaking_fee_recipient()
    fee_recipient = await pick_fee_recipient()

    data = (
        BUY_DISCRIMINATOR
        + struct.pack("<QQ", amount_tokens, max_sol_cost_lamports)
        + TRACK_VOLUME_TAIL
    )
    return Instruction(
        program_id=PUMP_PROGRAM_ID,
        data=data,
        accounts=[
            AccountMeta(GLOBAL_PDA, False, False),                  # 0
            AccountMeta(fee_recipient, False, True),                # 1
            AccountMeta(mint, False, False),                        # 2
            AccountMeta(bonding_curve, False, True),                # 3
            AccountMeta(associated_bonding_curve, False, True),     # 4
            AccountMeta(associated_user, False, True),              # 5
            AccountMeta(user, True, True),                          # 6
            AccountMeta(SYSTEM_PROGRAM_ID, False, False),           # 7
            AccountMeta(token_program, False, False),               # 8
            AccountMeta(creator_vault, False, True),                # 9
            AccountMeta(EVENT_AUTHORITY, False, False),             # 10
            AccountMeta(PUMP_PROGRAM_ID, False, False),             # 11
            AccountMeta(global_vol_acc, False, False),              # 12
            AccountMeta(user_vol_acc, False, True),                 # 13
            AccountMeta(fee_config, False, False),                  # 14
            AccountMeta(FEE_PROGRAM, False, False),                 # 15
            AccountMeta(bonding_curve_v2, False, False),            # 16
            AccountMeta(breaking_fee, False, True),                 # 17
        ],
    )


async def build_sell_ix(
    user: Pubkey,
    mint: Pubkey,
    amount_tokens: int,
    min_sol_out_lamports: int,
    creator: Pubkey,
    token_program: Pubkey = TOKEN_PROGRAM,
    cashback: bool = False,
) -> Instruction:
    """Build a Pump.fun sell instruction with the post-2026-04-28 layout.

    Cashback coins require `user_volume_accumulator` inserted before
    `bonding_curve_v2`. Detect this via byte 82 of the bonding curve account.
    """
    bonding_curve = derive_bonding_curve(mint)
    associated_bonding_curve = derive_associated_token_for_program(bonding_curve, mint, token_program)
    associated_user = derive_associated_token_for_program(user, mint, token_program)
    creator_vault = derive_creator_vault(creator)
    fee_config = derive_fee_config()
    bonding_curve_v2 = derive_bonding_curve_v2(mint)
    breaking_fee = _pick_breaking_fee_recipient()
    fee_recipient = await pick_fee_recipient()

    data = (
        SELL_DISCRIMINATOR
        + struct.pack("<QQ", amount_tokens, min_sol_out_lamports)
        + TRACK_VOLUME_TAIL
    )
    accounts = [
        AccountMeta(GLOBAL_PDA, False, False),                  # 0
        AccountMeta(fee_recipient, False, True),                # 1
        AccountMeta(mint, False, False),                        # 2
        AccountMeta(bonding_curve, False, True),                # 3
        AccountMeta(associated_bonding_curve, False, True),     # 4
        AccountMeta(associated_user, False, True),              # 5
        AccountMeta(user, True, True),                          # 6
        AccountMeta(SYSTEM_PROGRAM_ID, False, False),           # 7
        AccountMeta(creator_vault, False, True),                # 8
        AccountMeta(token_program, False, False),               # 9
        AccountMeta(EVENT_AUTHORITY, False, False),             # 10
        AccountMeta(PUMP_PROGRAM_ID, False, False),             # 11
        AccountMeta(fee_config, False, False),                  # 12
        AccountMeta(FEE_PROGRAM, False, False),                 # 13
    ]
    if cashback:
        user_vol_acc = derive_user_volume_accumulator(user)
        accounts.append(AccountMeta(user_vol_acc, False, True))  # +1 cashback
    accounts.append(AccountMeta(bonding_curve_v2, False, False))
    accounts.append(AccountMeta(breaking_fee, False, True))
    return Instruction(
        program_id=PUMP_PROGRAM_ID,
        data=data,
        accounts=accounts,
    )


async def send_versioned_tx(
    keypair, instructions: list, priority_fee_microlamports: int = 500_000,
    compute_unit_limit: int = 200_000,
    confirm_timeout_s: float = 25.0,
) -> str:
    """Build, sign, send, and CONFIRM a versioned transaction. Returns signature.

    Why we confirm instead of fire-and-forget:
    - `skipPreflight=True` + `finalized` blockhash is a known foot-gun that
      silently drops txs when the blockhash expires before the leader picks
      them up. We were seeing ~50% of buy sigs never appear on-chain.
    - We now request a `confirmed` blockhash (fresher, ~60s landing window)
      and poll `getSignatureStatuses` until the tx lands or times out.
    - If the tx doesn't confirm within `confirm_timeout_s`, raise — the caller
      treats this as a failed entry and won't open a phantom trade.
    """
    ixs = [
        set_compute_unit_limit(compute_unit_limit),
        set_compute_unit_price(priority_fee_microlamports),
        *instructions,
    ]
    # `confirmed` (~5s old) gives the leader ~60s vs `finalized` (~30s old)
    # which only gives ~30s — too tight when the network is busy.
    bh_res = await rpc_call("getLatestBlockhash", [{"commitment": "confirmed"}])
    blockhash_str = bh_res["result"]["value"]["blockhash"]
    from solders.hash import Hash
    blockhash = Hash.from_string(blockhash_str)
    msg = MessageV0.try_compile(
        payer=keypair.pubkey(),
        instructions=ixs,
        address_lookup_table_accounts=[],
        recent_blockhash=blockhash,
    )
    tx = VersionedTransaction(msg, [keypair])
    raw = bytes(tx)
    sig_res = await rpc_call(
        "sendTransaction",
        [
            base64.b64encode(raw).decode("utf-8"),
            {"encoding": "base64", "skipPreflight": True, "maxRetries": 3},
        ],
    )
    if "error" in sig_res:
        raise RuntimeError(f"sendTransaction error: {sig_res['error']}")
    sig = sig_res["result"]

    # Poll for confirmation. Pump.fun trades usually land in 2-8 seconds.
    import asyncio as _asyncio
    deadline = _asyncio.get_event_loop().time() + confirm_timeout_s
    poll = 0.6
    last_err = None
    while _asyncio.get_event_loop().time() < deadline:
        try:
            st_res = await rpc_call("getSignatureStatuses", [[sig], {"searchTransactionHistory": False}])
            value = ((st_res.get("result") or {}).get("value") or [None])[0]
            if value:
                err = value.get("err")
                conf = value.get("confirmationStatus") or ""
                if err is not None:
                    last_err = err
                    break
                if conf in ("confirmed", "finalized"):
                    # CRITICAL: getSignatureStatuses returns the tx-level err
                    # (signature/blockhash failure) but is BLIND to
                    # InstructionError (Custom:XXXX, IncorrectProgramId, etc.).
                    # A tx can "land + confirm" yet still have failed every
                    # instruction. Verify via getTransaction.meta.err — the
                    # only RPC field that exposes instruction-level errors.
                    # Without this check the bot treats failed-on-chain buys
                    # as successful entries, then "exits" empty positions —
                    # paying gas twice for a position that never existed.
                    try:
                        tx_res = await rpc_call(
                            "getTransaction",
                            [sig, {"encoding": "json",
                                   "commitment": "confirmed",
                                   "maxSupportedTransactionVersion": 0}],
                        )
                        tx_obj = tx_res.get("result")
                        if tx_obj:
                            meta_err = ((tx_obj.get("meta") or {}).get("err"))
                            if meta_err is not None:
                                last_err = meta_err
                                break
                    except Exception:
                        # If we can't verify, fall back to old behavior
                        # rather than wrongly mark the tx as failed.
                        pass
                    # Record per-recipient health (4.1 weighted picker)
                    for _ix in instructions:
                        if getattr(_ix, "program_id", None) == PUMP_PROGRAM_ID:
                            record_ix_outcome(_ix, True)
                    return sig
        except Exception as e:
            last_err = e
        await _asyncio.sleep(poll)
        poll = min(poll * 1.3, 2.0)

    # Record failure for all pump.fun ixs in the payload
    for _ix in instructions:
        if getattr(_ix, "program_id", None) == PUMP_PROGRAM_ID:
            record_ix_outcome(_ix, False)

    if last_err is not None:
        raise RuntimeError(f"tx {sig[:16]}… landed but failed: {last_err}")
    raise RuntimeError(f"tx {sig[:16]}… not confirmed in {confirm_timeout_s}s (blockhash likely expired)")
