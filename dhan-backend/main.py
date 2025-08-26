from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dhanhq import dhanhq

app = FastAPI()

# Allow CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🔑 Credentials (keep them safe!)
DHAN_CLIENT_ID = "1107860004"
DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU2ODM2NDA4LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwNzg2MDAwNCJ9.3cuzgiY0Qm2Id8wpMW0m90_ZxJ0TJRTV5fZ0tpAwWo3S1Mv5HbpcDNwXxXVepnOUHMRDck_AbArIoVOmlA68Dg"

dhan = None
try:
    dhan = dhanhq(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
except Exception as e:
    print(f"❌ Failed to initialize Dhan client: {e}")


@app.get("/status")
def status():
    if dhan is None:
        return {"status": "failed", "message": "Dhan client not initialized"}
    try:
        funds = dhan.get_fund_limits()
        return {
            "status": "success",
            "dhan_connection": "connected",
            "funds": funds,
        }
    except Exception as e:
        return {
            "status": "failed",
            "message": f"Error fetching funds: {str(e)}",
        }


@app.get("/holdings")
def holdings():
    if dhan is None:
        return {"status": "failed", "message": "Dhan client not initialized"}
    try:
        holdings = dhan.get_holdings()
        if holdings.get("status") == "success":
            return {
                "status": "success",
                "holdings": holdings.get("data", []),
            }
        else:
            return {
                "status": "failed",
                "message": holdings.get("remarks", "Unknown error"),
            }
    except Exception as e:
        return {"status": "failed", "message": f"Error fetching holdings: {str(e)}"}


@app.get("/positions")
def positions():
    if dhan is None:
        return {"status": "failed", "message": "Dhan client not initialized"}
    try:
        positions = dhan.get_positions()
        if positions.get("status") == "success":
            return {
                "status": "success",
                "positions": positions.get("data", []),
            }
        else:
            return {
                "status": "failed",
                "message": positions.get("remarks", "Unknown error"),
            }
    except Exception as e:
        return {"status": "failed", "message": f"Error fetching positions: {str(e)}"}
