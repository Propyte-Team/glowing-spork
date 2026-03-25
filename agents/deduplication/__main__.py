"""Permite ejecutar: python -m agents.deduplication (equivale a setup)."""
from .setup import main
import asyncio

if __name__ == "__main__":
    asyncio.run(main())
