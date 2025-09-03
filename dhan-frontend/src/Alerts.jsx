import React, { useEffect, useState } from "react";

export default function Alerts() {
  const [alerts, setAlerts] = useState([]);

  const fetchAlerts = async () => {
    try {
      const res = await fetch("http://127.0.0.1:8000/webhook/alerts");
      const data = await res.json();
      if (data.status === "success") setAlerts(data.alerts);
    } catch (e) {
      console.error("Failed to fetch alerts", e);
    }
  };

  useEffect(() => {
    fetchAlerts();
    const id = setInterval(fetchAlerts, 5000); // refresh every 5s
    return () => clearInterval(id);
  }, []);

  return (
    <div className="p-4">
      <h2 className="text-xl font-bold mb-4">Webhook Alerts</h2>
      <div className="space-y-3">
        {alerts.map((a, i) => (
          <div
            key={i}
            className={`p-4 rounded-xl shadow ${
              a.response.status === "success"
                ? "bg-green-800"
                : "bg-red-800"
            }`}
          >
            <div className="text-sm text-gray-300">Time: {a.timestamp}</div>
            <div className="font-semibold">
              {a.request.index} {a.request.strike} {a.request.option_type}{" "}
              {a.request.side}
            </div>
            <div className="text-sm">
              Lots: {a.request.lots || "-"} | Qty: {a.response.preview?.qty || "-"}
            </div>
            <div className="text-sm">
              Status: {a.response.status} | Message:{" "}
              {a.response.message || "N/A"}
            </div>
            {a.response.broker && (
              <pre className="text-xs bg-black/40 p-2 mt-2 rounded">
                {JSON.stringify(a.response.broker, null, 2)}
              </pre>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
