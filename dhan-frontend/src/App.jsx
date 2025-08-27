import { useEffect, useState } from "react";
import axios from "axios";

function App() {
  const [status, setStatus] = useState("Checking...");
  const [funds, setFunds] = useState(null);
  const [holdings, setHoldings] = useState([]);
  const [positions, setPositions] = useState([]);
  const [error, setError] = useState(null);
  const [marketOpen, setMarketOpen] = useState(null);
  const [orders, setOrders] = useState([]);

  async function fetchMarketStatus(segment) {
    try {
      const res = await axios.get("http://127.0.0.1:8000/market-status", { params: { segment }});
      if (res.data.status === "success") {
        setMarketOpen(res.data.isOpen);
      } else {
        setMarketOpen(null);
      }
    } catch {
      setMarketOpen(null);
    }
  }

  useEffect(() => {
    const fetchData = async () => {
      try {
        // status
        const res = await axios.get("http://127.0.0.1:8000/status");
        if (res.data.status === "success") {
          setStatus("Connected ✅");
          setFunds(res.data.funds?.data || {});
          setError(null);

          // holdings
          try {
            const holdingsRes = await axios.get("http://127.0.0.1:8000/holdings");
            if (holdingsRes.data.status === "success") {
              setHoldings(holdingsRes.data.holdings || []);
            } else {
              setHoldings([]);
              setError(holdingsRes.data.message || "Failed to load holdings");
            }
          } catch (err) {
            setHoldings([]);
            setError("Error fetching holdings");
          }

          // positions
          try {
            const positionsRes = await axios.get("http://127.0.0.1:8000/positions");
            if (positionsRes.data.status === "success") {
              setPositions(positionsRes.data.positions || []);
            } else {
              setPositions([]);
              setError(positionsRes.data.message || "Failed to load positions");
            }
          } catch (err) {
            setPositions([]);
            setError("Error fetching positions");
          }
        } else {
          setStatus("Failed ❌");
          setError(res.data.message || "Connection failed");
        }
      } catch (err) {
        setStatus("Server Error ❌");
        setError("Backend not reachable");
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 10000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="min-h-screen bg-gray-900 text-white flex flex-col items-center justify-start p-8 space-y-6">
      <h1 className="text-3xl font-bold">🚀 Dhan Dashboard</h1>

      <div className="bg-gray-800 p-6 rounded-2xl shadow-lg w-full max-w-2xl text-center">
        <h2 className="text-xl font-semibold mb-2">Connection Status</h2>
        <p className={status.includes("✅") ? "text-green-400" : "text-red-400"}>
          {status}
        </p>
      </div>

      {error && (
        <div className="bg-red-800 p-4 rounded-xl w-full max-w-2xl text-center">
          ⚠️ {error}
        </div>
      )}

      {/* Funds */}
      <div className="bg-gray-800 p-6 rounded-2xl shadow-lg text-left w-full max-w-2xl">
        <h2 className="text-xl font-bold mb-4 text-green-400">💰 Funds</h2>
        {funds ? (
          <ul>
            <li>Available: ₹{funds.availabelBalance ?? 0}</li>
            <li>Withdrawable: ₹{funds.withdrawableBalance ?? 0}</li>
          </ul>
        ) : (
          <p className="text-gray-400">No funds data</p>
        )}
      </div>

      {/* Holdings */}
      <div className="bg-gray-800 p-6 rounded-2xl shadow-lg text-left w-full max-w-2xl">
        <h2 className="text-xl font-bold mb-4 text-yellow-400">📊 Holdings</h2>
        {holdings.length > 0 ? (
          <table className="w-full text-left">
            <thead>
              <tr>
                <th className="p-2">Symbol</th>
                <th className="p-2">Qty</th>
                <th className="p-2">Avg Price</th>
                <th className="p-2">LTP</th>
                <th className="p-2">P&L</th>
              </tr>
            </thead>
            <tbody>
              {holdings.map((h, i) => {
                const pnl = (h.lastTradedPrice - h.avgCostPrice) * h.totalQty;
                return (
                  <tr key={i} className="border-t border-gray-700">
                    <td className="p-2">{h.tradingSymbol}</td>
                    <td className="p-2">{h.totalQty}</td>
                    <td className="p-2">₹{h.avgCostPrice?.toFixed(2)}</td>
                    <td className="p-2">₹{h.lastTradedPrice?.toFixed(2)}</td>
                    <td
                      className={`p-2 ${
                        pnl >= 0 ? "text-green-400" : "text-red-400"
                      }`}
                    >
                      ₹{pnl.toFixed(2)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        ) : (
          <p className="text-gray-400">No holdings found</p>
        )}
      </div>

      {/* Positions */}
      {positions && positions.length > 0 ? (
        <div className="bg-gray-800 p-6 rounded-2xl shadow-lg text-left w-full max-w-2xl">
          <h2 className="text-xl font-bold mb-4 text-green-400">📈 Positions</h2>
          <table className="w-full text-left">
            <thead>
              <tr>
                <th className="p-2">Symbol</th>
                <th className="p-2">Side</th>
                <th className="p-2">Qty</th>
                <th className="p-2">Avg Price</th>
                <th className="p-2">LTP</th>
                <th className="p-2">P&L</th>
              </tr>
            </thead>
            <tbody>
              {positions.map((p, i) => {
                const pnl = (p.lastTradedPrice - p.avgCostPrice) * p.totalQty;
                return (
                  <tr key={i} className="border-t border-gray-700">
                    <td className="p-2">{p.tradingSymbol}</td>
                    <td className="p-2">{p.transactionType}</td>
                    <td className="p-2">{p.totalQty}</td>
                    <td className="p-2">₹{p.avgCostPrice?.toFixed(2) || "N/A"}</td>
                    <td className="p-2">₹{p.lastTradedPrice?.toFixed(2) || "N/A"}</td>
                    <td className={`p-2 ${pnl >= 0 ? "text-green-400" : "text-red-400"}`}>
                      ₹{pnl?.toFixed(2) || "N/A"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="text-gray-400">No positions found</p>
      )}

      {/* Orders Section */}
      <div className="bg-gray-800 p-6 rounded-2xl shadow-lg w-full max-w-3xl mt-6">
        <h2 className="text-xl font-bold mb-4 text-yellow-400">📝 Place Order</h2>

        <form
          className="grid grid-cols-2 md:grid-cols-6 gap-3 items-end"
          onSubmit={async (e) => {
            e.preventDefault();
            const form = e.target;

            const segment = form.segment.value;
            const securityId = form.securityId.value.trim();
            const side = form.side.value;
            const qty = parseInt(form.qty.value, 10);
            const orderType = form.orderType.value;
            const price = orderType === "LIMIT" ? parseFloat(form.price.value || "0") : 0;
            const productType = form.productType.value;
            const validity = form.validity.value;

            // (Optional) re-check market status for this segment
            await fetchMarketStatus(segment);

            try {
              const res = await axios.post("http://127.0.0.1:8000/order/place", {
                segment,
                securityId,
                transactionType: side,
                quantity: qty,
                orderType,
                price,
                productType,
                validity,
              });

              if (res.data.status === "success") {
                const prev = res.data.preview;
                alert(
                  `✅ Order Placed\n` +
                  `Segment: ${prev.segment}\n` +
                  `SecurityId: ${prev.securityId}\n` +
                  `Side: ${prev.transactionType}\n` +
                  `Qty: ${prev.quantity}\n` +
                  `Type: ${prev.orderType}\n` +
                  `Price: ${prev.price}\n` +
                  `${res.data.marketNotice || ""}`
                );
              } else {
                alert(`❌ ${res.data.message}\n${res.data.marketNotice || ""}`);
              }
            } catch (err) {
              alert(`❌ ${err.message}`);
            }
          }}
        >
          <select name="segment" className="p-2 rounded bg-gray-700 text-white" onChange={(e)=>fetchMarketStatus(e.target.value)}>
            <option value="NSE_EQ">NSE Equity</option>
            <option value="BSE_EQ">BSE Equity</option>
            <option value="NSE_FNO">NSE F&O</option>
            <option value="MCX">MCX</option>
          </select>

          <input
            name="securityId"
            placeholder="securityId (e.g. 3499)"
            className="p-2 rounded bg-gray-700 text-white"
            required
          />

          <select name="side" className="p-2 rounded bg-gray-700 text-white">
            <option value="BUY">BUY</option>
            <option value="SELL">SELL</option>
          </select>

          <input
            type="number"
            name="qty"
            min="1"
            defaultValue="1"
            className="p-2 rounded bg-gray-700 text-white"
            required
          />

          <select name="orderType" className="p-2 rounded bg-gray-700 text-white" onChange={(e)=>{
            const showPrice = e.target.value === "LIMIT";
            const priceEl = document.getElementById("limitPriceField");
            if (priceEl) priceEl.style.display = showPrice ? "block" : "none";
          }}>
            <option value="MARKET">MARKET</option>
            <option value="LIMIT">LIMIT</option>
          </select>

          <input
            id="limitPriceField"
            name="price"
            type="number"
            step="0.05"
            placeholder="Limit Price"
            className="p-2 rounded bg-gray-700 text-white"
            style={{ display: "none" }}
          />

          <select name="productType" className="p-2 rounded bg-gray-700 text-white">
            <option value="DELIVERY">DELIVERY/CNC</option>
            <option value="INTRADAY">INTRADAY/MIS</option>
          </select>

          <select name="validity" className="p-2 rounded bg-gray-700 text-white">
            <option value="DAY">DAY</option>
            <option value="IOC">IOC</option>
          </select>

          <button type="submit" className="bg-green-600 px-4 py-2 rounded hover:bg-green-700 col-span-2 md:col-span-1">
            Place
          </button>
        </form>

        {/* Market status banner */}
        <div className="mt-4 text-sm">
          {marketOpen === true && <span className="text-green-400">✅ Market appears OPEN (clock check)</span>}
          {marketOpen === false && <span className="text-yellow-400">⚠️ Market appears CLOSED (clock check)</span>}
          {marketOpen === null && <span className="text-gray-400">ℹ️ Market status unavailable</span>}
        </div>

        {/* Order Book */}
        <div className="mt-6">
          <div className="flex items-center justify-between mb-2">
            <h3 className="font-semibold">Order Book</h3>
            <button
              className="bg-blue-600 px-3 py-1 rounded hover:bg-blue-700"
              onClick={async () => {
                try {
                  const res = await axios.get("http://127.0.0.1:8000/orders");
                  if (res.data.status === "success") {
                    setOrders(res.data.orders || []);
                  } else {
                    alert(`❌ ${res.data.message}`);
                  }
                } catch (err) {
                  alert(`❌ ${err.message}`);
                }
              }}
            >
              Refresh
            </button>
          </div>

          {orders.length > 0 ? (
            <table className="w-full text-left">
              <thead>
                <tr>
                  <th className="p-2">Order ID</th>
                  <th className="p-2">Symbol</th>
                  <th className="p-2">Side</th>
                  <th className="p-2">Qty</th>
                  <th className="p-2">Type</th>
                  <th className="p-2">Status</th>
                  <th className="p-2">Action</th>
                </tr>
              </thead>
              <tbody>
                {orders.map((o, i) => (
                  <tr key={i} className="border-t border-gray-700">
                    <td className="p-2">{o.orderId}</td>
                    <td className="p-2">{o.tradingSymbol || o.securityId}</td>
                    <td className="p-2">{o.transactionType}</td>
                    <td className="p-2">{o.quantity}</td>
                    <td className="p-2">{o.orderType}</td>
                    <td className="p-2">{o.orderStatus}</td>
                    <td className="p-2">
                      {String(o.orderStatus).toUpperCase().includes("PENDING") && (
                        <button
                          className="bg-red-600 px-3 py-1 rounded hover:bg-red-700"
                          onClick={async () => {
                            try {
                              const res = await axios.post("http://127.0.0.1:8000/order/cancel", null, {
                                params: { order_id: o.orderId },
                              });
                              alert(res.data.status === "success" ? "✅ Cancelled" : "❌ " + res.data.message);
                            } catch (err) {
                              alert("❌ " + err.message);
                            }
                          }}
                        >
                          Cancel
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <p className="text-gray-400">No orders found</p>
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
