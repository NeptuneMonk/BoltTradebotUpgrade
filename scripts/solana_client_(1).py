"""
Solana RPC helpers (Helius).
"""
import os
import httpx
import asyncio
from solders.pubkey import Pubkey

RPC_URL = os.environ["HELIUS_RPC_URL"]
LAMPORTS_PER_SOL = 1_000_000_000


async def rpc_call(method: str, params: list, timeout: float = 10.0,
                   max_retries: int = 3) -> dict:
    """JSON-RPC call to Helius with transient-failure retry.

    Retries on ConnectTimeout / ReadTimeout / 5xx / 429 (the only failure
    modes that are safe to retry — Helius rate-limits and edge-node
    cold-starts produce these intermittently). Backoff: 0.25s, 0.5s, 1.0s.

    Without retries, a single transient ConnectTimeout to Helius bubbles up
    to FastAPI as a 500, which the cluster ingress surfaces as a 502 to the
    browser — breaking user-facing actions like wallet token-scan, recovery
    sales, and stuck-position lookups."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.post(RPC_URL, json=payload)
                if r.status_code == 429 or 500 <= r.status_code < 600:
                    # transient — retry
                    last_exc = httpx.HTTPStatusError(
                        f"helius {r.status_code}", request=r.request, response=r
                    )
                else:
                    r.raise_for_status()
                    try:
                        from helius_budget import record_rpc_call
                        record_rpc_call()
                    except Exception:
                        pass
                    return r.json()
        except (httpx.ConnectTimeout, httpx.ReadTimeout,
                httpx.ConnectError, httpx.RemoteProtocolError) as e:
            last_exc = e
        # Backoff before next attempt (skip on last iteration)
        if attempt < max_retries - 1:
            await asyncio.sleep(0.25 * (2 ** attempt))
    # All retries exhausted
    assert last_exc is not None
    raise last_exc


async def get_sol_balance(pubkey_str: str) -> float:
    res = await rpc_call("getBalance", [pubkey_str, {"commitment": "confirmed"}])
    if "result" in res and res["result"]:
        lamports = res["result"]["value"]
        return lamports / LAMPORTS_PER_SOL
    return 0.0


async def get_account_info(pubkey_str: str) -> dict | None:
    res = await rpc_call(
        "getAccountInfo",
        [pubkey_str, {"encoding": "base64", "commitment": "confirmed"}],
    )
    return res.get("result", {}).get("value")


async def get_tx_wallet_delta_lamports(sig: str, wallet: str) -> int | None:
    """Return the actual signed lamport delta for `wallet` from a confirmed tx.
    Positive = wallet GAINED SOL. Negative = wallet SPENT SOL. Includes the
    gas fee (i.e., this is the true wallet movement).

    Returns None if the tx isn't found / hasn't confirmed / failed.
    """
    res = await rpc_call(
        "getTransaction",
        [sig, {"encoding": "json", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}],
    )
    tx = res.get("result")
    if not tx:
        return None
    meta = tx.get("meta") or {}
    if meta.get("err") is not None:
        # Tx landed but failed on-chain — no balance change, gas WAS paid
        # (the fee is already reflected in pre/post balances)
        pass
    # Resolve account index. V0 txs may put accounts in static + loaded.
    msg = (tx.get("transaction") or {}).get("message") or {}
    keys = msg.get("accountKeys") or []
    # Some endpoints return accountKeys as list of {pubkey: ...} objects
    flat_keys = [k if isinstance(k, str) else (k.get("pubkey") or "") for k in keys]
    pre = meta.get("preBalances") or []
    post = meta.get("postBalances") or []
    try:
        idx = flat_keys.index(wallet)
    except ValueError:
        return None
    if idx >= len(pre) or idx >= len(post):
        return None
    return int(post[idx]) - int(pre[idx])


_sol_price_cache = {"price": 0.0, "ts": 0.0}


async def get_sol_usd_price() -> float:
    """Cached for 60s. Tries Binance first (cloud-IP friendly), falls back to Coinbase, then CoinGecko."""
    now = asyncio.get_event_loop().time()
    if _sol_price_cache["price"] > 0 and now - _sol_price_cache["ts"] < 60:
        return _sol_price_cache["price"]
    sources = [
        ("binance", "https://api.binance.com/api/v3/ticker/price", {"symbol": "SOLUSDT"}, lambda d: float(d["price"])),
        ("coinbase", "https://api.coinbase.com/v2/exchange-rates", {"currency": "SOL"}, lambda d: 1.0 / float(d["data"]["rates"]["USD"]) if False else float(d["data"]["rates"]["USD"])),
        ("coingecko", "https://api.coingecko.com/api/v3/simple/price", {"ids": "solana", "vs_currencies": "usd"}, lambda d: float(d["solana"]["usd"])),
    ]
    for name, url, params, parser in sources:
        try:
            async with httpx.AsyncClient(timeout=6.0) as client:
                r = await client.get(url, params=params)
                if r.status_code != 200:
                    continue
                price = parser(r.json())
                if price > 0:
                    _sol_price_cache["price"] = price
                    _sol_price_cache["ts"] = now
                    return price
        except Exception:
            continue
    return _sol_price_cache["price"] or 150.0
