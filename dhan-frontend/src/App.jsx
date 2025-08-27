import React, { useEffect, useRef, useState } from "react";
import axios from "axios";

/**
 * Config
 * - Backend assumed at http://127.0.0.1:8000
 * - If your backend is elsewhere, change BASE_URL
 */
const BASE_URL = "http://127.0.0.1:8000";

function safeMsg(x) {
  if (!x && x !== 0) return "";
  if (typeof x === "string") return x;
  if (typeof x === "object") {
    if (x.message) return String(x.message);
    try {
      return JSON.stringify(x);
    } catch {
      return String(x);
    }
  }
  return String(x);
}

export default function App() {
  // Connection / global
  const [status, setStatus] = useState("Checking...");
  const [statusError, setStatusError] = useState(null);

  // Data
  const [funds, setFunds] = useState(null);
  const [holdings, setHoldings] = useState([]);
  const [positions, setPositions] = useState([]);
  const [orders, setOrders] = useState([]);

  // Orders / form
  const [segment, setSegment] = useState("NSE_EQ");
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState([]);
  const [selectedInstrument, setSelectedInstrument] = useState(null);
  const [qty, setQty] = useState(1);
  const [side, setSide] = useState("BUY");
  const [orderType, setOrderType] = useState("MARKET");
  const [price, setPrice] = useState("");
  const [productType, setProductType] = useState("DELIVERY");
  const [validity, setValidity] = useState("DAY");

  // UI state
  const [error, setError] = useState(null);
  const [placing, setPlacing] = useState(false);
  const [loadingOrders, setLoadingOrders] = useState(false);
  const [marketNotice, setMarketNotice] = useState("");

  // refs for intervals / debounce
  const pollRef = useRef(null);
  const searchDebounceRef = useRef(null);

  // Create a single axios instance
  const api = axios.create({
    baseURL: BASE_URL,
    timeout: 10000,
  });

  // Fetch status + data
  const fetchAll = async () => {
    try {
      // 1) status
      const statusRes = await api.get("/status");
      if (statusRes?.data?.status === "ok") {
        setStatus("Connected ✅");
        setStatusError(null);
      } else {
        setStatus("Not Connected ❌");
        setStatusError(statusRes?.data?.message || "Backend reported not-ok");
      }

      // 2) funds
      try {
        const f = await api.get("/funds");
        if (f.data?.status === "success") {
          setFunds(f.data.funds ?? f.data);
        } else {
          setFunds(null);
          setError(f.data?.message ?? "Failed to load funds");
        }
      } catch (e) {
        setFunds(null);
        setError("Error fetching funds: " + safeMsg(e.message || e));
      }

      // 3) holdings
      try {
        const h = await api.get("/holdings");
        if (h.data?.status === "success") {
          // holdings might be under h.data.holdings or h.data.data - normalize
          const data = h.data.holdings ?? h.data.data ?? [];
          setHoldings(Array.isArray(data) ? data : []);
        } else {
          setHoldings([]);
          setError(h.data?.message ?? "Dhan API failure");
        }
              } catch (e) {
          setHoldings([]);
          setError("Backend unreachable");
        }

      // 4) positions
      try {
        const p = await api.get("/positions");
        if (p.data?.status === "success") {
          const data = p.data.positions ?? p.data.data ?? [];
          setPositions(Array.isArray(data) ? data : []);
        } else {
          setPositions([]);
          setError(p.data?.message ?? "Dhan API failure");
        }
              } catch (e) {
          setPositions([]);
          setError("Backend unreachable");
        }
    } catch (err) {
      // Overall failure (backend unreachable or CORS)
      setStatus("Not Connected ❌");
      setStatusError(safeMsg(err.message || err));
      setError("Backend unreachable: " + safeMsg(err.message || err));
    }
  };

  // Polling start/stop
  useEffect(() => {
    fetchAll(); // initial
    pollRef.current = setInterval(fetchAll, 10000); // every 10s
    return () => {
      clearInterval(pollRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Orders fetch
  const fetchOrders = async () => {
    setLoadingOrders(true);
    try {
      const r = await api.get("/orders");
      if (r.data?.status === "success") {
        setOrders(r.data.orders ?? r.data.data ?? []);
      } else {
        setOrders([]);
        setError(r.data?.message ?? "Failed to load orders");
      }
    } catch (e) {
      setOrders([]);
      setError("Error fetching orders: " + safeMsg(e.message || e));
    } finally {
      setLoadingOrders(false);
    }
  };

  useEffect(() => {
    // load orders once on mount and refresh every 10s
    fetchOrders();
    const id = setInterval(fetchOrders, 10000);
    return () => clearInterval(id);
  }, []);

  // symbol search (debounced)
  useEffect(() => {
    if (!searchQuery || searchQuery.length < 2) {
      setSearchResults([]);
      return;
    }
    if (searchDebounceRef.current) clearTimeout(searchDebounceRef.current);
    searchDebounceRef.current = setTimeout(async () => {
      try {
        const res = await api.get("/symbol-search", { params: { query: searchQuery, segment } });
        if (res.data?.status === "success") {
          setSearchResults(res.data.results ?? []);
        } else {
          setSearchResults([]);
        }
      } catch (e) {
        setSearchResults([]);
      }
    }, 300);
    return () => {
      if (searchDebounceRef.current) clearTimeout(searchDebounceRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchQuery, segment]);

  // handle selecting instrument from search results
  const pickInstrument = (inst) => {
    setSelectedInstrument(inst);
    setSearchQuery(inst.tradingSymbol + (inst.expiry ? ` ${inst.expiry}` : ""));
    setSearchResults([]);
  };

  // place order
  const handlePlaceOrder = async (e) => {
    e.preventDefault();
    setPlacing(true);
    setError(null);
    setMarketNotice("");
    if (!selectedInstrument) {
      setError("Please select an instrument from search results first.");
      setPlacing(false);
      return;
    }
    try {
      const params = {
        symbol: selectedInstrument.tradingSymbol,
        qty: qty,
        side: side,
        segment: segment,
        order_type: orderType,
        price: orderType === "LIMIT" ? parseFloat(price || 0) : 0,
        product_type: productType,
        validity: validity
      };

      // Backend expects query params on POST; we use axios.post with params (null body)
      const res = await api.post("/order/place", null, { params });
      if (res.data?.status === "success") {
        setMarketNotice(res.data.marketNotice || "");
        // show success preview
        const preview = res.data.preview || {};
        alert(
          "Order placed ✅\n" +
          `Symbol: ${preview.symbol || selectedInstrument.tradingSymbol}\n` +
          `Qty: ${preview.qty || qty}\n` +
          `Side: ${preview.side || side}\n` +
          `Type: ${preview.order_type || orderType}\n` +
          (res.data.marketNotice ? "\n" + res.data.marketNotice : "")
        );
        // refresh orders & holdings/positions
        fetchOrders();
        fetchAll();
      } else {
        const msg = res.data?.message || safeMsg(res.data);
        setError("Order failed: " + msg);
        setMarketNotice(res.data?.marketNotice || "");
      }
    } catch (e) {
      setError("Error placing order: " + safeMsg(e.message || e));
    } finally {
      setPlacing(false);
    }
  };

  // cancel order
  const handleCancel = async (orderId) => {
    if (!orderId) return;
    if (!window.confirm(`Cancel order ${orderId}?`)) return;
    try {
      const res = await api.post("/order/cancel", null, { params: { order_id: orderId } });
      if (res.data?.status === "success") {
        alert("Order cancelled ✅");
        fetchOrders();
      } else {
        alert("Cancel failed: " + (res.data?.message || safeMsg(res.data)));
      }
    } catch (e) {
      alert("Cancel error: " + safeMsg(e.message || e));
    }
  };

  // small helpers for display
  const money = (v) => (v === null || v === undefined ? "-" : Number(v).toFixed(2));

  return (
    <div className="min-h-screen bg-gray-900 text-white p-6 font-sans">
      <div className="max-w-5xl mx-auto space-y-6">
        <header className="flex items-center justify-between">
          <h1 className="text-2xl font-bold">Dhan Dashboard</h1>
          <div className="text-right">
            <div className="text-sm">Connection</div>
            <div className={`font-mono ${status.includes("Connected") ? "text-green-400" : "text-red-400"}`}>
              {status}
            </div>
            {statusError && <div className="text-xs text-yellow-300 mt-1">{statusError}</div>}
          </div>
        </header>

        {error && (
          <div className="bg-red-800 p-3 rounded text-sm">
            <strong>Error:</strong> {safeMsg(error)}
          </div>
        )}

        {/* Funds */}
        <section className="bg-gray-800 p-4 rounded shadow">
          <h2 className="font-semibold text-lg text-green-300">💰 Funds</h2>
          {funds ? (
            <div className="grid grid-cols-2 gap-4 mt-2 text-sm">
              <div>Available: ₹{money(funds.availabelBalance ?? funds.available ?? funds.avail)}</div>
              <div>Withdrawable: ₹{money(funds.withdrawableBalance ?? funds.withdrawable ?? funds.withdraw)}</div>
              <div>SOD Limit: ₹{money(funds.sodLimit ?? funds.sodLimit)}</div>
              <div>Collateral: ₹{money(funds.collateralAmount ?? funds.collateral)}</div>
            </div>
          ) : (
            <div className="text-gray-400 text-sm mt-2">Funds not available</div>
          )}
        </section>

        {/* Holdings */}
        <section className="bg-gray-800 p-4 rounded shadow">
          <h2 className="font-semibold text-lg text-yellow-300">📦 Holdings</h2>
          {holdings.length > 0 ? (
            <div className="overflow-auto mt-2">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-gray-300">
                    <th className="p-2">Symbol</th>
                    <th className="p-2">Qty</th>
                    <th className="p-2">Avg</th>
                    <th className="p-2">LTP</th>
                    <th className="p-2">P&L</th>
                  </tr>
                </thead>
                <tbody>
                  {holdings.map((h, i) => {
                    const avg = parseFloat(h.avgCostPrice ?? h.averagePrice ?? 0);
                    const ltp = parseFloat(h.lastTradedPrice ?? h.ltp ?? 0);
                    const qtyv = parseFloat(h.totalQty ?? h.quantity ?? 0);
                    const pnl = (ltp - avg) * qtyv;
                    return (
                      <tr key={i} className="border-t border-gray-700">
                        <td className="p-2">{h.tradingSymbol}</td>
                        <td className="p-2">{qtyv}</td>
                        <td className="p-2">₹{money(avg)}</td>
                        <td className="p-2">₹{money(ltp)}</td>
                        <td className={`p-2 ${pnl >= 0 ? "text-green-400" : "text-red-400"}`}>₹{money(pnl)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-gray-400 mt-2">No holdings found</div>
          )}
        </section>

        {/* Positions */}
        <section className="bg-gray-800 p-4 rounded shadow">
          <h2 className="font-semibold text-lg text-purple-300">📌 Positions</h2>
          {positions.length > 0 ? (
            <div className="overflow-auto mt-2">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-gray-300">
                    <th className="p-2">Symbol</th>
                    <th className="p-2">Qty</th>
                    <th className="p-2">Avg</th>
                    <th className="p-2">LTP</th>
                    <th className="p-2">P&L</th>
                  </tr>
                </thead>
                <tbody>
                  {positions.map((p, i) => {
                    const avg = parseFloat(p.buyAvg ?? p.avgPrice ?? 0);
                    const ltp = parseFloat(p.ltp ?? p.lastTradedPrice ?? 0);
                    const qtyv = parseFloat(p.netQty ?? p.quantity ?? 0);
                    const pnl = (ltp - avg) * qtyv;
                    return (
                      <tr key={i} className="border-t border-gray-700">
                        <td className="p-2">{p.tradingSymbol}</td>
                        <td className="p-2">{qtyv}</td>
                        <td className="p-2">₹{money(avg)}</td>
                        <td className="p-2">₹{money(ltp)}</td>
                        <td className={`p-2 ${pnl >= 0 ? "text-green-400" : "text-red-400"}`}>₹{money(pnl)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-gray-400 mt-2">No open positions</div>
          )}
        </section>

        {/* Orders & Place Order */}
        <section className="bg-gray-800 p-4 rounded shadow">
          <div className="flex items-start justify-between gap-4">
            <h2 className="font-semibold text-lg text-yellow-400">📝 Orders</h2>

            <div className="text-sm">
              <button
                className="bg-blue-600 px-3 py-1 rounded text-white mr-2"
                onClick={fetchOrders}
                disabled={loadingOrders}
              >
                Refresh Orders
              </button>
              <button
                className="bg-gray-700 px-3 py-1 rounded text-white"
                onClick={() => {
                  setOrders([]);
                  fetchOrders();
                }}
              >
                Clear & Refresh
              </button>
            </div>
          </div>

          {/* Place order form */}
          <form className="mt-3 grid grid-cols-1 md:grid-cols-6 gap-2 items-end" onSubmit={handlePlaceOrder}>
            <div className="md:col-span-2">
              <label className="block text-xs text-gray-400">Segment</label>
              <select className="w-full p-2 rounded bg-gray-700" value={segment} onChange={(e) => setSegment(e.target.value)}>
                <option value="NSE_EQ">NSE Equity</option>
                <option value="BSE_EQ">BSE Equity</option>
                <option value="NSE_FNO">NSE F&O</option>
                <option value="MCX">MCX</option>
              </select>
            </div>

            <div className="md:col-span-2">
              <label className="block text-xs text-gray-400">Symbol / Search</label>
              <input
                type="text"
                className="w-full p-2 rounded bg-gray-700"
                placeholder="Type symbol (e.g. TCS or NIFTY24SEP...)"
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setSelectedInstrument(null);
                }}
              />
              {searchResults.length > 0 && (
                <ul className="bg-gray-800 mt-1 rounded max-h-44 overflow-auto border border-gray-700">
                  {searchResults.map((s) => (
                    <li
                      key={s.securityId}
                      className="p-2 hover:bg-gray-700 cursor-pointer text-sm"
                      onClick={() => pickInstrument(s)}
                    >
                      <div className="flex justify-between">
                        <div>{s.tradingSymbol} {s.expiry ? `(${s.expiry})` : ""}</div>
                        <div className="text-xs text-gray-400">ID:{s.securityId}</div>
                      </div>
                      <div className="text-xs text-gray-400">{s.segment} • lot: {s.lotSize}</div>
                    </li>
                  ))}
                </ul>
              )}
            </div>

            <div>
              <label className="block text-xs text-gray-400">Qty</label>
              <input type="number" min="1" value={qty} onChange={(e) => setQty(Number(e.target.value || 1))} className="p-2 rounded bg-gray-700 w-full" />
            </div>

            <div>
              <label className="block text-xs text-gray-400">Side</label>
              <select value={side} onChange={(e) => setSide(e.target.value)} className="p-2 rounded bg-gray-700 w-full">
                <option value="BUY">BUY</option>
                <option value="SELL">SELL</option>
              </select>
            </div>

            <div>
              <label className="block text-xs text-gray-400">Order Type</label>
              <select value={orderType} onChange={(e) => setOrderType(e.target.value)} className="p-2 rounded bg-gray-700 w-full">
                <option value="MARKET">MARKET</option>
                <option value="LIMIT">LIMIT</option>
              </select>
            </div>

            {orderType === "LIMIT" && (
              <div className="md:col-span-1">
                <label className="block text-xs text-gray-400">Limit Price</label>
                <input type="number" step="0.01" value={price} onChange={(e) => setPrice(e.target.value)} className="p-2 rounded bg-gray-700 w-full" />
              </div>
            )}

            <div className="md:col-span-6 flex gap-2 mt-2">
              <select value={productType} onChange={(e)=>setProductType(e.target.value)} className="p-2 rounded bg-gray-700">
                <option value="DELIVERY">DELIVERY/CNC</option>
                <option value="INTRADAY">INTRADAY/MIS</option>
              </select>
              <select value={validity} onChange={(e)=>setValidity(e.target.value)} className="p-2 rounded bg-gray-700">
                <option value="DAY">DAY</option>
                <option value="IOC">IOC</option>
              </select>
              <button type="submit" className="bg-green-600 px-4 py-2 rounded hover:bg-green-700" disabled={placing}>
                {placing ? "Placing..." : "Place Order"}
              </button>

              <div className="ml-auto text-sm text-gray-400">
                {selectedInstrument ? (
                  <div>
                    <div><strong>Selected:</strong> {selectedInstrument.tradingSymbol} (ID: {selectedInstrument.securityId})</div>
                    <div className="text-xs">Segment: {selectedInstrument.segment} • lot: {selectedInstrument.lotSize}</div>
                  </div>
                ) : (
                  <div className="text-xs">No instrument selected</div>
                )}
              </div>
            </div>
          </form>

          {marketNotice && <div className="mt-2 text-yellow-300 text-sm">{marketNotice}</div>}
          <hr className="my-3 border-gray-700" />

          {/* Order Book */}
          <div>
            <h3 className="font-semibold">Order Book</h3>
            {loadingOrders ? (
              <div className="text-sm text-gray-400 mt-2">Loading orders...</div>
            ) : orders && orders.length > 0 ? (
              <div className="overflow-auto mt-2">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-gray-300">
                      <th className="p-2">Order ID</th>
                      <th className="p-2">Symbol</th>
                      <th className="p-2">Side</th>
                      <th className="p-2">Qty</th>
                      <th className="p-2">Status</th>
                      <th className="p-2">Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {orders.map((o, i) => (
                      <tr key={i} className="border-t border-gray-700">
                        <td className="p-2">{o.orderId || o.id || o.clientOrderId}</td>
                        <td className="p-2">{o.tradingSymbol || o.securityId || "-"}</td>
                        <td className="p-2">{o.transactionType || o.side || "-"}</td>
                        <td className="p-2">{o.quantity || o.qty || "-"}</td>
                        <td className="p-2">{o.orderStatus || o.status || "-"}</td>
                        <td className="p-2">
                          {(String(o.orderStatus || o.status || "").toUpperCase().includes("PENDING") ||
                            String(o.orderStatus || o.status || "").toUpperCase().includes("OPEN")) ? (
                            <button className="bg-red-600 px-3 py-1 rounded hover:bg-red-700 text-sm" onClick={() => handleCancel(o.orderId || o.id)}>
                              Cancel
                            </button>
                          ) : (
                            <span className="text-gray-400 text-sm">—</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-gray-400 mt-2">No orders found</div>
            )}
          </div>
        </section>

        <footer className="text-center text-xs text-gray-500">
          Polling every 10s • Backend: {BASE_URL}
        </footer>
      </div>
    </div>
  );
}
