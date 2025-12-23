"""
Admin panel routes - serves the HTML admin interface.
"""

from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import HTMLResponse, FileResponse

router = APIRouter()

TEMPLATES_DIR = Path(__file__).parent / "admin_panel" / "templates"
STATIC_DIR = Path(__file__).parent / "admin_panel" / "static"


@router.get("/admin", response_class=HTMLResponse)
async def admin_panel():
    """Serve the admin panel HTML."""
    html_file = TEMPLATES_DIR / "index.html"
    if html_file.exists():
        return HTMLResponse(content=html_file.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>Admin panel not found</h1>", status_code=404)


@router.get("/admin/static/{filename}")
async def admin_static(filename: str):
    """Serve static files for admin panel."""
    file_path = STATIC_DIR / filename
    if file_path.exists():
        return FileResponse(file_path)
    return HTMLResponse(content="Not found", status_code=404)
