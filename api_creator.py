"""
API endpoint for fetching creator registry data.
External clients can query all creators or filter by agency.
"""

from flask import Flask, jsonify, request, make_response
from flask_cors import CORS
import os
import psycopg2
import psycopg2.extras
from functools import wraps
import csv
import io
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Database configuration
DB_HOST = os.getenv("PG_HOST", "localhost")
DB_PORT = os.getenv("PG_PORT", "5432")
DB_NAME = os.getenv("PG_DB", "adtree")
DB_USER = os.getenv("PG_USER", "postgres")
DB_PASSWORD = os.getenv("PG_PASSWORD", "4dtr33")

# API Key for authentication (set via environment variable)
API_KEY = os.getenv("API_KEY", "default-api-key-change-in-production")


def get_connection():
    """Create a database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def require_api_key(f):
    """Decorator to require API key authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get("X-API-Key")
        if not api_key or api_key != API_KEY:
            return jsonify({"error": "Unauthorized: Invalid or missing API key"}), 401
        return f(*args, **kwargs)
    return decorated_function


@app.route("/api/creators", methods=["GET"])
@require_api_key
def get_all_creators():
    """
    Fetch all creators from public.creator_registry.

    Optional query parameters:
    - agency_id: Filter by agency ID
    - limit: Number of records (default: 1000, max: 10000)
    - offset: Pagination offset (default: 0)
    - binding_status: Filter by "Bound" or "Unbound"

    Example: /api/creators?agency_id=1&limit=100&offset=0
    """
    try:
        # Get query parameters
        agency_id = request.args.get("agency_id", type=int)
        limit = min(int(request.args.get("limit", 1000)), 10000)  # Max 10000
        offset = int(request.args.get("offset", 0))
        binding_status = request.args.get("binding_status")

        conn = get_connection()
        cur = conn.cursor()

        # Build the query
        sql = """
            SELECT
                cr.id,
                cr.tiktok_id,
                cr.followers,
                cr.full_name,
                cr.domicile,
                cr.uid,
                cr.phone_number,
                cr.tiktok_link,
                cr.binding_status,
                cr.onboarding_date,
                cr.month_label,
                cr.level,
                cr.agency_id,
                am.agency_name,
                cr.created_at,
                cr.updated_at
            FROM public.creator_registry cr
            LEFT JOIN public.agency_map am ON cr.agency_id = am.id
            WHERE 1=1
        """
        params = []

        if agency_id:
            sql += " AND agency_id = %s"
            params.append(agency_id)

        if binding_status:
            sql += " AND binding_status = %s"
            params.append(binding_status)

        # Get total count
        count_sql = "SELECT COUNT(*) as total FROM public.creator_registry WHERE 1=1"
        count_params = []
        if agency_id:
            count_sql += " AND agency_id = %s"
            count_params.append(agency_id)
        if binding_status:
            count_sql += " AND binding_status = %s"
            count_params.append(binding_status)

        cur.execute(count_sql, count_params)
        total_count = cur.fetchone()["total"]

        # Add pagination
        sql += " ORDER BY cr.id DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        cur.execute(sql, params)
        creators = cur.fetchall()
        cur.close()
        conn.close()

        return jsonify({
            "success": True,
            "total": total_count,
            "limit": limit,
            "offset": offset,
            "count": len(creators),
            "data": [dict(row) for row in creators],
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/creators/by-agency/<int:agency_id>", methods=["GET"])
@require_api_key
def get_creators_by_agency(agency_id):
    """
    Fetch all creators for a specific agency.

    Example: /api/creators/by-agency/1
    """
    try:
        limit = min(int(request.args.get("limit", 1000)), 10000)
        offset = int(request.args.get("offset", 0))

        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT
                cr.id,
                cr.tiktok_id,
                cr.followers,
                cr.full_name,
                cr.domicile,
                cr.uid,
                cr.phone_number,
                cr.tiktok_link,
                cr.binding_status,
                cr.onboarding_date,
                cr.month_label,
                cr.level,
                cr.agency_id,
                am.agency_name,
                cr.created_at,
                cr.updated_at
            FROM public.creator_registry cr
            LEFT JOIN public.agency_map am ON cr.agency_id = am.id
            WHERE cr.agency_id = %s
            ORDER BY cr.id DESC
            LIMIT %s OFFSET %s
        """

        # Get total count
        count_sql = "SELECT COUNT(*) as total FROM public.creator_registry WHERE agency_id = %s"
        cur.execute(count_sql, (agency_id,))
        total_count = cur.fetchone()["total"]

        cur.execute(sql, (agency_id, limit, offset))
        creators = cur.fetchall()
        cur.close()
        conn.close()

        return jsonify({
            "success": True,
            "agency_id": agency_id,
            "total": total_count,
            "limit": limit,
            "offset": offset,
            "count": len(creators),
            "data": [dict(row) for row in creators],
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/health", methods=["GET"])
def health_check():
    """Health check endpoint (no auth required)."""
    return jsonify({"status": "ok", "service": "Creator Registry API"})


@app.route("/api/creators/export", methods=["GET"])
@require_api_key
def export_creators():
    """
    Bulk export all creators as JSON or CSV.

    Query parameters:
    - format: "json" (default) or "csv"
    - agency_id: Filter by agency ID (optional)
    - binding_status: Filter by "Bound" or "Unbound" (optional)

    Example: /api/creators/export?format=csv&agency_id=1
    """
    try:
        format_type = request.args.get("format", "json").lower()
        agency_id = request.args.get("agency_id", type=int)
        binding_status = request.args.get("binding_status")

        if format_type not in ["json", "csv"]:
            return jsonify({"success": False, "error": "Invalid format. Use 'json' or 'csv'"}), 400

        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT
                cr.id,
                cr.tiktok_id,
                cr.followers,
                cr.full_name,
                cr.domicile,
                cr.uid,
                cr.phone_number,
                cr.tiktok_link,
                cr.binding_status,
                cr.onboarding_date,
                cr.month_label,
                cr.level,
                cr.agency_id,
                am.agency_name,
                cr.created_at,
                cr.updated_at
            FROM public.creator_registry cr
            LEFT JOIN public.agency_map am ON cr.agency_id = am.id
            WHERE 1=1
        """
        params = []

        if agency_id:
            sql += " AND cr.agency_id = %s"
            params.append(agency_id)

        if binding_status:
            sql += " AND cr.binding_status = %s"
            params.append(binding_status)

        sql += " ORDER BY cr.id DESC"

        cur.execute(sql, params)
        creators = cur.fetchall()
        cur.close()
        conn.close()

        if format_type == "csv":
            # Generate CSV
            output = io.StringIO()
            if creators:
                fieldnames = list(creators[0].keys())
                writer = csv.DictWriter(output, fieldnames=fieldnames)
                writer.writeheader()
                for row in creators:
                    writer.writerow({key: row[key] for key in fieldnames})

            csv_content = output.getvalue()
            response = make_response(csv_content)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            response.headers["Content-Disposition"] = f"attachment; filename=creators_export_{timestamp}.csv"
            response.headers["Content-Type"] = "text/csv; charset=utf-8"
            return response

        else:
            # Return JSON
            return jsonify({
                "success": True,
                "count": len(creators),
                "format": "json",
                "exported_at": datetime.now().isoformat(),
                "data": [dict(row) for row in creators],
            })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/docs", methods=["GET"])
def docs():
    """API documentation."""
    return jsonify({
        "service": "Creator Registry API",
        "version": "1.0",
        "endpoints": [
            {
                "path": "/api/creators",
                "method": "GET",
                "auth": "Required (X-API-Key header)",
                "description": "Fetch all creators with pagination",
                "parameters": {
                    "agency_id": "integer (optional)",
                    "binding_status": "string: 'Bound' or 'Unbound' (optional)",
                    "limit": "integer, default 1000, max 10000",
                    "offset": "integer, default 0"
                }
            },
            {
                "path": "/api/creators/by-agency/<agency_id>",
                "method": "GET",
                "auth": "Required (X-API-Key header)",
                "description": "Fetch creators for a specific agency with pagination",
                "parameters": {
                    "limit": "integer, default 1000, max 10000",
                    "offset": "integer, default 0"
                }
            },
            {
                "path": "/api/creators/export",
                "method": "GET",
                "auth": "Required (X-API-Key header)",
                "description": "Bulk export ALL creators as JSON or CSV (no pagination)",
                "parameters": {
                    "format": "string: 'json' (default) or 'csv'",
                    "agency_id": "integer (optional) - filter by agency",
                    "binding_status": "string: 'Bound' or 'Unbound' (optional) - filter by status"
                }
            },
            {
                "path": "/api/health",
                "method": "GET",
                "auth": "Not required",
                "description": "Health check"
            },
            {
                "path": "/api/docs",
                "method": "GET",
                "auth": "Not required",
                "description": "This documentation"
            }
        ]
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8006, debug=False)
