import React, { useEffect, useState } from "react";

export default function Alerts() {
  const [alerts, setAlerts] = useState([]);
  const [logs, setLogs] = useState([]);

  const fetchAlerts = async () => {
    try {
      const res = await fetch("http://127.0.0.1:8000/webhook/alerts");
      const data = await res.json();
      if (data.status === "success") {
        const now = Date.now();
        const updated = data.alerts.map(a => ({
          ...a,
          _expiry: now + 20000, // 20s lifetime
        }));
        // Dashboard ephemeral
        setAlerts(prev => {
          const merged = [...updated];
          return merged;
        });
        // Log persistent
        setLogs(prev => [...updated, ...prev].slice(0, 200));
      }
    } catch (e) {
      console.error("Failed to fetch alerts", e);
    }
  };

  useEffect(() => {
    fetchAlerts();
    const poll = setInterval(fetchAlerts, 5000);
    const clean = setInterval(() => {
      setAlerts(prev => prev.filter(a => Date.now() < a._expiry));
    }, 1000);
    return () => {
      clearInterval(poll);
      clearInterval(clean);
    };
  }, []);

  return (
    <div className="p-4 space-y-6">
      <div>
        <h2 className="text-xl font-bold mb-4">⚡ Live Alerts (20s)</h2>
        <div className="space-y-3">
          {alerts.map(a => (
            <div
              key={a.id}
              className={`p-4 rounded-xl shadow ${
                a.response.status === "success" ? "bg-green-800" : "bg-red-800"
              }`}
            >
              <div className="text-sm text-gray-300">
                Time: {a.timestamp}
              </div>
              <div className="font-semibold">
                {a.request.index} {a.request.strike} {a.request.option_type}{" "}
                {a.request.side}
              </div>
              <div className="text-sm">
                Lots: {a.request.lots || "-"} | Qty:{" "}
                {a.response.preview?.qty || "-"}
              </div>
              <div className="text-sm">
                Status: {a.response.status} | Message:{" "}
                {a.response.message || "N/A"}
              </div>
            </div>
          ))}
        </div>
      </div>

      <div>
        <h2 className="text-xl font-bold mb-4">📜 Logs (Persistent for Today)</h2>
        <div className="bg-gray-900 p-3 rounded-lg h-64 overflow-y-auto text-xs font-mono">
          {logs.map((a, i) => (
            <div key={i} className="mb-2">
              [{a.timestamp}] {a.request.index} {a.request.strike}
              {a.request.option_type} {a.request.side} →{" "}
              {a.response.status.toUpperCase()} | {a.response.message}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
