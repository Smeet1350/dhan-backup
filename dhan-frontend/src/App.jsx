import { useEffect, useState } from "react";
import axios from "axios";

function App() {
  const [status, setStatus] = useState("Checking...");
  const [funds, setFunds] = useState(null);
  const [holdings, setHoldings] = useState([]);
  const [positions, setPositions] = useState([]);
  const [error, setError] = useState(null);

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
      <div className="bg-gray-800 p-6 rounded-2xl shadow-lg text-left w-full max-w-2xl">
        <h2 className="text-xl font-bold mb-4 text-purple-400">📌 Positions</h2>
        {positions.length > 0 ? (
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
              {positions.map((p, i) => {
                const pnl = (p.ltp - p.buyAvg) * p.netQty;
                return (
                  <tr key={i} className="border-t border-gray-700">
                    <td className="p-2">{p.tradingSymbol}</td>
                    <td className="p-2">{p.netQty}</td>
                    <td className="p-2">₹{p.buyAvg?.toFixed(2)}</td>
                    <td className="p-2">₹{p.ltp?.toFixed(2)}</td>
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
          <p className="text-gray-400">No open positions</p>
        )}
      </div>
    </div>
  );
}

export default App;
