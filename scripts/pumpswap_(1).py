"""
PumpSwap AMM integration — for tokens that have graduated from the Pump.fun
bonding curve. PumpSwap is a constant-product AMM (x·y=k).

Program ID:  pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA
Pool layout (after 8-byte discriminator):
   1  pool_bump (u8)
   2  index     (u16)
  32  creator
  32  base_mint
  32  quote_mint     (always WSOL for SOL pairs)
  32  lp_mint
  32  pool_base_token_account
  32  pool_quote_token_account
   8  lp_supply (u64)
  32  coin_creator     <-- used as the creator for the creator_vault PDA

Fee: 0.20% LP + 0.05% protocol = 0.25% per trade.

This module mirrors `pumpfun.py` in structure so the bot can route by protocol.
"""
from __future__ import annotations

import base64
import os
import random
import struct
from typing import Optional

from solders.pubkey import Pubkey
from solders.instruction import Instruction, AccountMeta
from solders.system_program import ID as SYSTEM_PROGRAM_ID, CreateAccountWithSeedParams, create_account_with_seed
from spl.token.instructions import (
    InitializeAccountParams,
    CloseAccountParams,
    initialize_account,
    close_account,
    get_associated_token_address,
)

from solana_client import rpc_call, LAMPORTS_PER_SOL

# ---- Program & system addresses ----
PUMPSWAP_PROGRAM_ID = Pubkey.from_string("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA")
GLOBAL_CONFIG = Pubkey.from_string("ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw")
# Standard fee recipient (non-mayhem-mode pools). Confirmed against
# chainstack's reference + on-chain pump-swap pools.
PROTOCOL_FEE_RECIPIENT = Pubkey.from_string("7VtfL8fvgNfhz17qKRMjzQEXgbdpnHHHQRh54R9jP2RJ")
# WSOL ATA owned by PROTOCOL_FEE_RECIPIENT (computed deterministically).
PROTOCOL_FEE_RECIPIENT_TOKEN_ACCOUNT = Pubkey.from_string("7GFUN3bWzJMKMRZ34JLsvcqdssDbXnp589SiE33KVwcC")
FEE_PROGRAM = Pubkey.from_string("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ")
EVENT_AUTH = Pubkey.from_string("GS4CU59F31iL7aR2Q8zVS8DRrcRnXX1yjQ66TqNVQnaR")
GLOBAL_VOL_ACC = Pubkey.from_string("C2aFPdENg4A2HQsmrd5rTw5TaYBX5Ku887cWjbFKtZpw")
TOKEN_PROGRAM = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
ASSOCIATED_TOKEN_PROGRAM = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
WSOL = Pubkey.from_string("So11111111111111111111111111111111111111112")
ACCOUNT_SPACE = 165  # SPL token account size in bytes
WSOL_RENT_LAMPORTS = 2_039_280  # min-balance rent for a 165-byte token account

# Anchor instruction discriminators
BUY_DISCRIMINATOR = bytes.fromhex("66063d1201daebea")
SELL_DISCRIMINATOR = bytes.fromhex("33e685a4017f83ad")

# Fees (basis points)
LP_FEE_BPS = 20      # 0.20%
PROTOCOL_FEE_BPS = 5  # 0.05%
TOTAL_FEE_BPS = LP_FEE_BPS + PROTOCOL_FEE_BPS  # 25 bps


def derive_creator_vault(coin_creator: Pubkey) -> Pubkey:
    pda, _ = Pubkey.find_program_address(
        [b"creator_vault", bytes(coin_creator)], PUMPSWAP_PROGRAM_ID
    )
    return pda


def derive_user_volume_accumulator(user: Pubkey) -> Pubkey:
    pda, _ = Pubkey.find_program_address(
        [b"user_volume_accumulator", bytes(user)], PUMPSWAP_PROGRAM_ID
    )
    return pda


def derive_fee_config() -> Pubkey:
    pda, _ = Pubkey.find_program_address(
        [b"fee_config", bytes(PUMPSWAP_PROGRAM_ID)], FEE_PROGRAM
    )
    return pda


def derive_pool_v2(base_mint: Pubkey) -> Pubkey:
    """Per-base-mint pool-v2 PDA. Required as the LAST 'pre-upgrade' account
    on every PumpSwap buy/sell from the 2026-04-28 program upgrade onward.
    Without this account, the program reverts with Custom:6023 (Overflow)."""
    pda, _ = Pubkey.find_program_address(
        [b"pool-v2", bytes(base_mint)], PUMPSWAP_PROGRAM_ID
    )
    return pda


# PumpSwap-specific breaking-fee recipients (DIFFERENT from pumpfun's bonding
# curve list). Per pump-public-docs/BREAKING_FEE_RECIPIENT.md. Using the wrong
# list reverts with Custom:6053 (BuybackFeeRecipientNotAuthorized).
BREAKING_FEE_RECIPIENTS_PS = [
    Pubkey.from_string(s) for s in (
        "5YxQFdt3Tr9zJLvkFccqXVUwhdTWJQc1fFg2YPbxvxeD",
        "9M4giFFMxmFGXtc3feFzRai56WbBqehoSeRE5GK7gf7",
        "GXPFM2caqTtQYC2cJ5yJRi9VDkpsYZXzYdwYpGnLmtDL",
        "3BpXnfJaUTiwXnJNe7Ej1rcbzqTTQUvLShZaWazebsVR",
        "5cjcW9wExnJJiqgLjq7DEG75Pm6JBgE1hNv4B2vHXUW6",
        "EHAAiTxcdDwQ3U4bU6YcMsQGaekdzLS3B5SmYo46kJtL",
        "5eHhjP8JaYkz83CWwvGU2uMUXefd3AazWGx4gpcuEEYD",
        "A7hAgCzFw14fejgCp387JUJRMNyz4j89JKnhtKU8piqW",
    )
]


# ---- Pool account decoder ----
async def fetch_pool_state(pool_address: str) -> Optional[dict]:
    """Decode a PumpSwap pool account. Returns reserves + key addresses."""
    pool = Pubkey.from_string(pool_address)
    res = await rpc_call(
        "getAccountInfo",
        [str(pool), {"encoding": "base64", "commitment": "confirmed"}],
    )
    val = res.get("result", {}).get("value")
    if not val:
        return None
    raw = base64.b64decode(val["data"][0])
    # 8 disc + 1 bump + 2 index + 32 creator = 43
    # base_mint @ 43, quote_mint @ 75, lp_mint @ 107,
    # pool_base_token_account @ 139, pool_quote_token_account @ 171,
    # lp_supply @ 203 (u64), coin_creator @ 211
    if len(raw) < 211 + 32:
        return None
    base_mint = Pubkey.from_bytes(raw[43:75])
    quote_mint = Pubkey.from_bytes(raw[75:107])
    pool_base = Pubkey.from_bytes(raw[139:171])
    pool_quote = Pubkey.from_bytes(raw[171:203])
    coin_creator = Pubkey.from_bytes(raw[211:243])
    # Per pump-public-docs: byte 243 = is_mayhem_mode, byte 244 = is_cashback.
    # Both affect the IX layout (extra accounts) and fee recipient resolution.
    is_mayhem_mode = bool(raw[243]) if len(raw) > 243 else False
    is_cashback = bool(raw[244]) if len(raw) > 244 else False

    # Fetch the two vaults' balances
    bal_res = await rpc_call(
        "getMultipleAccounts",
        [[str(pool_base), str(pool_quote)], {"encoding": "jsonParsed", "commitment": "confirmed"}],
    )
    accts = bal_res.get("result", {}).get("value") or []
    if len(accts) < 2 or not accts[0] or not accts[1]:
        return None
    try:
        base_amt = int(accts[0]["data"]["parsed"]["info"]["tokenAmount"]["amount"])
        base_decimals = int(accts[0]["data"]["parsed"]["info"]["tokenAmount"]["decimals"])
        quote_amt = int(accts[1]["data"]["parsed"]["info"]["tokenAmount"]["amount"])
    except (KeyError, TypeError, ValueError):
        return None
    return {
        "pool": str(pool),
        "base_mint": str(base_mint),
        "quote_mint": str(quote_mint),
        "pool_base_token_account": str(pool_base),
        "pool_quote_token_account": str(pool_quote),
        "coin_creator": str(coin_creator),
        "base_reserves": base_amt,
        "quote_reserves": quote_amt,
        "base_decimals": base_decimals,
        # For symmetry with pumpfun's state dict
        "real_sol_reserves": quote_amt,  # WSOL vault balance == real SOL liquidity
        "complete": False,                # AMM pools don't "complete"
        "is_mayhem_mode": is_mayhem_mode,
        "is_cashback": is_cashback,
    }


def price_sol_per_raw_token(state: dict) -> float:
    """Current price (SOL per raw token unit), invariant in the same units as
    pumpfun.fetch_bonding_curve_state's price calc."""
    base = state.get("base_reserves") or 0
    quote = state.get("quote_reserves") or 0
    if base <= 0 or quote <= 0:
        return 0.0
    return quote / base / LAMPORTS_PER_SOL


def quote_buy_tokens(state: dict, sol_in_lamports: int, slippage_bps: int = 500) -> tuple[int, int]:
    """SOL → tokens. Returns (expected_tokens_out, max_sol_cost_with_slippage)."""
    base_r = state["base_reserves"]
    quote_r = state["quote_reserves"]
    if base_r <= 0 or quote_r <= 0 or sol_in_lamports <= 0:
        return 0, sol_in_lamports
    # tokens_out = base_r - (base_r * quote_r) / (quote_r + sol_in)
    new_quote = quote_r + sol_in_lamports
    new_base = (base_r * quote_r) // new_quote
    tokens_out = base_r - new_base
    # NB: PumpSwap charges fees on the SOL side. Use a slight tokens_out reduction.
    tokens_out_after_fee = tokens_out - (tokens_out * TOTAL_FEE_BPS) // 10_000
    max_sol = sol_in_lamports + (sol_in_lamports * slippage_bps) // 10_000
    return tokens_out_after_fee, max_sol


def quote_sell_sol(state: dict, tokens_in: int, slippage_bps: int = 500) -> tuple[int, int]:
    """Tokens → SOL. Returns (expected_sol_out_after_fees, min_sol_out_with_slippage)."""
    base_r = state["base_reserves"]
    quote_r = state["quote_reserves"]
    if base_r <= 0 or quote_r <= 0 or tokens_in <= 0:
        return 0, 0
    new_base = base_r + tokens_in
    new_quote = (base_r * quote_r) // new_base
    sol_out_gross = quote_r - new_quote
    fees = (sol_out_gross * TOTAL_FEE_BPS) // 10_000
    sol_out = sol_out_gross - fees
    min_sol = sol_out - (sol_out * slippage_bps) // 10_000
    return sol_out, max(0, min_sol)


# ---- Pool lookup by mint (used when discovery only gives us the mint) ----
async def find_pool_for_mint(mint_str: str) -> Optional[str]:
    """Find the highest-liquidity PumpSwap pool for a given base mint paired
    with WSOL. Uses getProgramAccounts with the canonical filter offsets."""
    wsol_str = "So11111111111111111111111111111111111111112"
    candidates: list[str] = []
    for base_off, quote_off, base_b58, quote_b58 in (
        (43, 75, mint_str, wsol_str),
        (43, 75, wsol_str, mint_str),
    ):
        res = await rpc_call(
            "getProgramAccounts",
            [
                str(PUMPSWAP_PROGRAM_ID),
                {
                    "encoding": "base64",
                    "commitment": "confirmed",
                    "filters": [
                        {"memcmp": {"offset": base_off, "bytes": base_b58}},
                        {"memcmp": {"offset": quote_off, "bytes": quote_b58}},
                    ],
                },
            ],
        )
        for acc in (res.get("result") or []):
            try:
                candidates.append(acc["pubkey"])
            except (KeyError, TypeError):
                continue
    if not candidates:
        return None
    # Probe each pool's reserves and pick the highest-liquidity one.
    best, best_liq = None, 0
    for pool in candidates:
        state = await fetch_pool_state(pool)
        if not state:
            continue
        liq = state["base_reserves"] * state["quote_reserves"]
        if liq > best_liq:
            best_liq = liq
            best = pool
    return best


# ---- Instruction builders ----
def build_buy_ix(
    user: Pubkey,
    state: dict,
    user_token_account: Pubkey,
    user_wsol_account: Pubkey,
    base_amount_out: int,
    max_quote_amount_in: int,
    base_token_program: Pubkey | None = None,
) -> Instruction:
    """Build a PumpSwap `buy` instruction. Expects `state` from fetch_pool_state.
    base_token_program: token program owning the *base mint* — defaults to
    TOKEN_PROGRAM (TokenKEG). Set TOKEN_2022 if the mint uses it."""
    base_token_program = base_token_program or TOKEN_PROGRAM
    pool = Pubkey.from_string(state["pool"])
    base_mint = Pubkey.from_string(state["base_mint"])
    quote_mint = Pubkey.from_string(state["quote_mint"])
    pool_base = Pubkey.from_string(state["pool_base_token_account"])
    pool_quote = Pubkey.from_string(state["pool_quote_token_account"])
    coin_creator = Pubkey.from_string(state["coin_creator"])
    creator_vault_authority = derive_creator_vault(coin_creator)
    creator_vault_ata = get_associated_token_address(creator_vault_authority, WSOL, TOKEN_PROGRAM)
    user_vol_acc = derive_user_volume_accumulator(user)
    fee_config = derive_fee_config()
    # 2026-04-28 upgrade additions
    bf_recipient = random.choice(BREAKING_FEE_RECIPIENTS_PS)
    bf_quote_ata = get_associated_token_address(bf_recipient, WSOL, TOKEN_PROGRAM)
    pool_v2 = derive_pool_v2(base_mint)

    keys = [
        AccountMeta(pool, False, True),
        AccountMeta(user, True, True),
        AccountMeta(GLOBAL_CONFIG, False, False),
        AccountMeta(base_mint, False, False),
        AccountMeta(quote_mint, False, False),
        AccountMeta(user_token_account, False, True),
        AccountMeta(user_wsol_account, False, True),
        AccountMeta(pool_base, False, True),
        AccountMeta(pool_quote, False, True),
        AccountMeta(PROTOCOL_FEE_RECIPIENT, False, False),
        AccountMeta(PROTOCOL_FEE_RECIPIENT_TOKEN_ACCOUNT, False, True),
        AccountMeta(base_token_program, False, False),
        AccountMeta(TOKEN_PROGRAM, False, False),
        AccountMeta(SYSTEM_PROGRAM_ID, False, False),
        AccountMeta(ASSOCIATED_TOKEN_PROGRAM, False, False),
        AccountMeta(EVENT_AUTH, False, False),
        AccountMeta(PUMPSWAP_PROGRAM_ID, False, False),
        AccountMeta(creator_vault_ata, False, True),
        AccountMeta(creator_vault_authority, False, False),
        AccountMeta(GLOBAL_VOL_ACC, False, True),
        AccountMeta(user_vol_acc, False, True),
        AccountMeta(fee_config, False, False),
        AccountMeta(FEE_PROGRAM, False, False),
        # 2026-04-28 upgrade: pool-v2 PDA + 2 breaking-fee accounts at end.
        # Without these the program reverts with Custom:6023 (Overflow).
        AccountMeta(pool_v2, False, False),
        AccountMeta(bf_recipient, False, False),
        AccountMeta(bf_quote_ata, False, True),
    ]
    data = BUY_DISCRIMINATOR + struct.pack("<QQ", base_amount_out, max_quote_amount_in)
    return Instruction(program_id=PUMPSWAP_PROGRAM_ID, data=data, accounts=keys)


def build_sell_ix(
    user: Pubkey,
    state: dict,
    user_token_account: Pubkey,
    user_wsol_account: Pubkey,
    base_amount_in: int,
    min_quote_amount_out: int,
    base_token_program: Pubkey | None = None,
) -> Instruction:
    base_token_program = base_token_program or TOKEN_PROGRAM
    pool = Pubkey.from_string(state["pool"])
    base_mint = Pubkey.from_string(state["base_mint"])
    quote_mint = Pubkey.from_string(state["quote_mint"])
    pool_base = Pubkey.from_string(state["pool_base_token_account"])
    pool_quote = Pubkey.from_string(state["pool_quote_token_account"])
    coin_creator = Pubkey.from_string(state["coin_creator"])
    creator_vault_authority = derive_creator_vault(coin_creator)
    creator_vault_ata = get_associated_token_address(creator_vault_authority, WSOL, TOKEN_PROGRAM)
    fee_config = derive_fee_config()
    # 2026-04-28 upgrade additions
    bf_recipient = random.choice(BREAKING_FEE_RECIPIENTS_PS)
    bf_quote_ata = get_associated_token_address(bf_recipient, WSOL, TOKEN_PROGRAM)
    pool_v2 = derive_pool_v2(base_mint)
    is_cashback = bool(state.get("is_cashback", False))
    # Cashback variant adds 2 writable accounts BEFORE pool_v2:
    # user_volume_accumulator + its WSOL ATA
    user_vol_acc_cb = derive_user_volume_accumulator(user) if is_cashback else None
    user_vol_acc_quote_ata = (
        get_associated_token_address(user_vol_acc_cb, WSOL, TOKEN_PROGRAM)
        if is_cashback else None
    )

    keys = [
        AccountMeta(pool, False, True),
        AccountMeta(user, True, True),
        AccountMeta(GLOBAL_CONFIG, False, False),
        AccountMeta(base_mint, False, False),
        AccountMeta(quote_mint, False, False),
        AccountMeta(user_token_account, False, True),
        AccountMeta(user_wsol_account, False, True),
        AccountMeta(pool_base, False, True),
        AccountMeta(pool_quote, False, True),
        AccountMeta(PROTOCOL_FEE_RECIPIENT, False, False),
        AccountMeta(PROTOCOL_FEE_RECIPIENT_TOKEN_ACCOUNT, False, True),
        AccountMeta(base_token_program, False, False),
        AccountMeta(TOKEN_PROGRAM, False, False),
        AccountMeta(SYSTEM_PROGRAM_ID, False, False),
        AccountMeta(ASSOCIATED_TOKEN_PROGRAM, False, False),
        AccountMeta(EVENT_AUTH, False, False),
        AccountMeta(PUMPSWAP_PROGRAM_ID, False, False),
        AccountMeta(creator_vault_ata, False, True),
        AccountMeta(creator_vault_authority, False, False),
        AccountMeta(fee_config, False, False),
        AccountMeta(FEE_PROGRAM, False, False),
    ]
    if is_cashback:
        # Both writable, BEFORE pool_v2
        keys.extend([
            AccountMeta(user_vol_acc_quote_ata, False, True),
            AccountMeta(user_vol_acc_cb, False, True),
        ])
    keys.extend([
        # 2026-04-28 upgrade: pool-v2 PDA + 2 breaking-fee accounts at end.
        # Without these the program reverts with Custom:6023 (Overflow).
        AccountMeta(pool_v2, False, False),
        AccountMeta(bf_recipient, False, False),
        AccountMeta(bf_quote_ata, False, True),
    ])
    data = SELL_DISCRIMINATOR + struct.pack("<QQ", base_amount_in, min_quote_amount_out)
    return Instruction(program_id=PUMPSWAP_PROGRAM_ID, data=data, accounts=keys)


def build_wsol_wrap_ixs(user: Pubkey, lamports: int) -> tuple[Pubkey, list[Instruction]]:
    """Create a temporary WSOL token account funded with `lamports`. Returns
    (wsol_account_pubkey, instructions). Caller is responsible for closing the
    account at the end of the tx with `build_close_wsol_ix`.

    NOTE: This temp-account flow is for BUY (we need to deposit SOL).
    For PumpSwap SELL, use `build_wsol_ata_idempotent_ixs` instead — pump-swap's
    sell instruction requires `user_wsol_account` to be the user's deterministic
    WSOL ATA, NOT a seed-derived temp account, or it reverts with Custom:6053
    (constraint seeds mismatch)."""
    seed = base64.urlsafe_b64encode(os.urandom(24)).decode("utf-8")
    wsol_account = Pubkey.create_with_seed(user, seed, TOKEN_PROGRAM)
    create_ix = create_account_with_seed(
        CreateAccountWithSeedParams(
            from_pubkey=user,
            to_pubkey=wsol_account,
            base=user,
            seed=seed,
            lamports=WSOL_RENT_LAMPORTS + lamports,
            space=ACCOUNT_SPACE,
            owner=TOKEN_PROGRAM,
        )
    )
    init_ix = initialize_account(
        InitializeAccountParams(
            program_id=TOKEN_PROGRAM,
            account=wsol_account,
            mint=WSOL,
            owner=user,
        )
    )
    return wsol_account, [create_ix, init_ix]


def build_close_wsol_ix(user: Pubkey, wsol_account: Pubkey) -> Instruction:
    return close_account(
        CloseAccountParams(
            program_id=TOKEN_PROGRAM,
            account=wsol_account,
            dest=user,
            owner=user,
        )
    )


def build_wsol_ata_idempotent_ixs(user: Pubkey) -> tuple[Pubkey, list[Instruction]]:
    """Return the user's deterministic WSOL ATA + a single idempotent-create IX.

    Use this for PumpSwap SELL — the program REQUIRES `user_wsol_account` to be
    the canonical ATA, not a seed-derived temp account. Sells revert with
    Custom:6053 (seeds mismatch) when given a temp account.

    For BUY you can use either, but the ATA path is also fine — we just don't
    need to pre-fund it with lamports because we'll write SOL via PumpSwap and
    receive base tokens out."""
    wsol_ata = get_associated_token_address(user, WSOL, TOKEN_PROGRAM)
    return wsol_ata, [build_create_ata_ix(user, user, WSOL, TOKEN_PROGRAM)]


def build_create_ata_ix(payer: Pubkey, owner: Pubkey, mint: Pubkey,
                        token_program: Pubkey | None = None) -> Instruction:
    """Create an ATA (idempotent — safe to call when account already exists).
    `token_program` defaults to classic SPL; pass Token-2022 for those mints
    otherwise the ATA address will be wrong and the buy IX will revert
    with IncorrectProgramId."""
    tp = token_program or TOKEN_PROGRAM
    ata = get_associated_token_address(owner, mint, tp)
    return Instruction(
        program_id=ASSOCIATED_TOKEN_PROGRAM,
        data=bytes([1]),  # CreateIdempotent
        accounts=[
            AccountMeta(payer, True, True),
            AccountMeta(ata, False, True),
            AccountMeta(owner, False, False),
            AccountMeta(mint, False, False),
            AccountMeta(SYSTEM_PROGRAM_ID, False, False),
            AccountMeta(tp, False, False),
        ],
    )


async def get_token_balance(ata: Pubkey) -> int:
    """Read the raw token amount currently held in an ATA. Returns 0 if account
    doesn't exist. Used before selling to size the trade by actual balance."""
    res = await rpc_call(
        "getTokenAccountBalance",
        [str(ata), {"commitment": "confirmed"}],
    )
    try:
        return int(res["result"]["value"]["amount"])
    except (KeyError, TypeError, ValueError):
        return 0
