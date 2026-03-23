"""Allow running as: python -m enrichment_agent_v2"""
from .main import main
import asyncio

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("\nStopped by user")
