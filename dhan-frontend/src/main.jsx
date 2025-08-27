import React from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import "./index.css"; // if you have tailwind built in, keep this. otherwise optional.

createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
