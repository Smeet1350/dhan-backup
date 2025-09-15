// src/Alerts.jsx
import React, { useEffect, useState } from "react";
import axios from "axios";

const BASE_URL = import.meta.env?.VITE_API_URL || `http://${window.location.hostname}:8000`;
const api = axios.create({ baseURL: BASE_URL, timeout: 30000 });

export default function Alerts() {
  const [alerts, setAlerts] = useState([]);
  const [err, setErr] = useState(null);

  const fetchAlerts = async () => {
    try {
      const r = await api.get("/webhook/alerts"); // correct path
      setAlerts(r.data?.alerts ?? []);
      setErr(null);
    } catch (e) {
      console.error("fetchAlerts failed", e);
      setErr("Could not load alerts");
    }
  };

  useEffect(() => {
    fetchAlerts();
    const id = setInterval(fetchAlerts, 10000);
    return () => clearInterval(id);
  }, []);

  return (
    <section className="card">
      <h2 className="font-semibold text-lg text-red-300">🚨 Webhook Alerts</h2>
      {err && <div className="text-sm text-yellow-300">{err}</div>}
      <div className="mt-2 text-sm max-h-48 overflow-auto">
        {alerts.length === 0 ? (
          <div className="text-gray-400">No alerts</div>
        ) : (
          <ul className="space-y-2">
            {alerts.map((a) => (
              <li key={a.id} className="bg-gray-800 p-2 rounded">
                <div className="text-xs text-gray-400">
                  {new Date(a.timestamp).toLocaleString()}
                </div>

                {a.trade && (
                  <div className="text-sm mt-1">
                    <strong>{a.trade.index}</strong> {a.trade.strike}{a.trade.option_type}
                    — {a.trade.side}
                    {a.lots ? ` ${a.lots} lot(s)` : ""}
                    {a.lot_size ? ` (lotSize=${a.lot_size})` : ""}
                    → Qty: {a.qty}
                    @ {a.trade.order_type}
                    {a.trade.price && a.trade.price > 0 ? ` (₹${a.trade.price})` : ""}
                  </div>
                )}

                <div className="text-xs mt-1 text-green-300">
                  {a.response?.message ?? JSON.stringify(a.response?.broker?.raw ?? {})}
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </section>
  );
}