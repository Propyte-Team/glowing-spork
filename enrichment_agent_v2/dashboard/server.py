"""
Dashboard web en tiempo real — FastAPI + WebSocket.
Con controles de start/stop para el agente de enriquecimiento.
"""

import asyncio
import json
import logging
import os
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

logger = logging.getLogger("enrichment_v2.dashboard")

# Connected WebSocket clients
_clients: set[WebSocket] = set()

# Reference to httpx client for API endpoints
_http_client = None

# Agent state
_agent_task: asyncio.Task | None = None
_agent_status = "idle"  # idle | running | stopping
_agent_config = {
    "strategies": ["geocode", "scrape", "search", "ai"],
    "limit": 50,
    "dry_run": False,
    "ciudad": None,
    "portal": None,
}

STATIC_DIR = Path(__file__).parent
INDEX_HTML = STATIC_DIR / "index.html"


async def broadcast(data: dict):
    """Envía un evento a todos los clientes WebSocket conectados."""
    global _clients
    if not _clients:
        return
    message = json.dumps(data, default=str)
    disconnected = set()
    for ws in list(_clients):
        try:
            await ws.send_text(message)
        except Exception:
            disconnected.add(ws)
    _clients -= disconnected


async def _run_agent_loop():
    """Ejecuta el agente de enriquecimiento como tarea de background."""
    global _agent_status
    _agent_status = "running"
    await broadcast({"type": "agent_status", "status": "running"})

    try:
        from ..main import run_once
        import argparse

        # Build args from config
        args = argparse.Namespace(
            geocode="geocode" in _agent_config["strategies"],
            scrape="scrape" in _agent_config["strategies"],
            search="search" in _agent_config["strategies"],
            ai="ai" in _agent_config["strategies"],
            limit=_agent_config["limit"],
            dry_run=_agent_config["dry_run"],
            no_dashboard=True,
            loop=False,
            ciudad=_agent_config["ciudad"],
            portal=_agent_config["portal"],
        )

        # If no specific strategy selected, run all
        if not (args.geocode or args.scrape or args.search or args.ai):
            args.geocode = args.scrape = args.search = args.ai = True

        stats = await run_once(args, _http_client)

        await broadcast({
            "type": "agent_status",
            "status": "completed",
            "stats": stats,
        })
    except asyncio.CancelledError:
        logger.info("Agent stopped by user")
        await broadcast({"type": "agent_status", "status": "stopped"})
    except Exception as e:
        import traceback
        logger.error(f"Agent error: {e}\n{traceback.format_exc()}")
        await broadcast({
            "type": "agent_status",
            "status": "error",
            "error": str(e),
        })
    finally:
        _agent_status = "idle"
        await broadcast({"type": "agent_status", "status": "idle"})


def create_app(http_client=None) -> FastAPI:
    """Crea la app FastAPI con el cliente HTTP inyectado."""
    global _http_client
    _http_client = http_client

    app = FastAPI(title="Propyte Enrichment Agent V2", docs_url=None, redoc_url=None)

    @app.get("/")
    async def root():
        return FileResponse(str(INDEX_HTML), media_type="text/html")

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        _clients.add(websocket)
        logger.info(f"Dashboard client connected ({len(_clients)} total)")
        # Send current status on connect
        try:
            await websocket.send_text(json.dumps({
                "type": "agent_status",
                "status": _agent_status,
                "config": _agent_config,
            }))
        except Exception:
            pass
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            _clients.discard(websocket)
            logger.info(f"Dashboard client disconnected ({len(_clients)} total)")

    @app.get("/api/coverage")
    async def api_coverage():
        """Retorna estadísticas de cobertura actuales."""
        if not _http_client:
            return JSONResponse({"error": "No HTTP client"}, status_code=503)
        try:
            from ..supabase_writer import get_coverage_stats, get_portal_coverage
            stats = await get_coverage_stats(_http_client)
            portals = await get_portal_coverage(_http_client)
            return JSONResponse({"stats": stats, "portals": portals})
        except Exception as e:
            logger.error(f"Coverage API error: {e}")
            return JSONResponse({"error": str(e)}, status_code=500)

    @app.get("/api/status")
    async def api_status():
        return JSONResponse({
            "status": _agent_status,
            "clients": len(_clients),
            "config": _agent_config,
        })

    @app.post("/api/start")
    async def api_start(config: dict = None):
        """Inicia el agente de enriquecimiento."""
        global _agent_task, _agent_config

        if _agent_status == "running":
            return JSONResponse({"error": "Agent already running"}, status_code=409)

        # Update config if provided
        if config:
            if "strategies" in config:
                _agent_config["strategies"] = config["strategies"]
            if "limit" in config:
                _agent_config["limit"] = min(int(config["limit"]), 500)
            if "dry_run" in config:
                _agent_config["dry_run"] = bool(config["dry_run"])
            if "ciudad" in config:
                _agent_config["ciudad"] = config["ciudad"] or None
            if "portal" in config:
                _agent_config["portal"] = config["portal"] or None

        _agent_task = asyncio.create_task(_run_agent_loop())
        return JSONResponse({"status": "started", "config": _agent_config})

    @app.post("/api/stop")
    async def api_stop():
        """Detiene el agente de enriquecimiento."""
        global _agent_task, _agent_status

        if _agent_status != "running" or _agent_task is None:
            return JSONResponse({"error": "Agent not running"}, status_code=409)

        _agent_status = "stopping"
        _agent_task.cancel()
        await broadcast({"type": "agent_status", "status": "stopping"})
        return JSONResponse({"status": "stopping"})

    return app
