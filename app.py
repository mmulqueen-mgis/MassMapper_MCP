#!/usr/bin/env python3
"""
AWS Lambda entry‑point for the MassGIS MCP action‑group
–– patched 2025‑07‑30 **rev‑6**
















Key fixes in this revision
-------------------------
* **Fixed longitude parameter** – maps 'longitude' to 'lon' for find_nearby
* **Improved quote handling** – better detection and fixing of unclosed quotes in CQL
* **CQL validation** – warns about unsupported SELECT statements
* **Fixed properties extraction** – now correctly handles the nested 'properties'
  key in application/json content
* **Added tool name aliases** – handles both camelCase and kebab-case API paths
* **Fixed parameter name mismatches** – maps 'layer_name' to 'lid' where needed
* **Event loop handling** – properly handles closed event loops in Lambda environment
* **Session reset on errors** – resets httpx session when event loop is closed
* **NPE on missing `actionGroupInvocationInput`** – we now coerce the variable
  to an **empty dict** when neither the nested nor the flat shape is present so
  attribute access never fails (resolves `AttributeError: 'NoneType' object`).
* **Broader shape detection** – handles `invocationInput` being **list *or* dict**.
* **Missing `query` / `limit` arguments** – put defaults *before* positional
  invocation so `_search_layers()` is always satisfied.
* **Robust path → tool mapping** – tolerate repeated `/` and allow either
  kebab‑case (`/search-layers`) *or* snake case (`/search_layers`).
* **Stricter numeric parsing** – accept integers passed as *either* strings or
  JSON numbers.
* **Extra logging** – DEBUG dump of the normalised argument dict and the tool
  name before invocation.
* **Version banner env‑var** – honour `APP_TS` so we can verify the running
  build from CloudWatch logs (emit on cold‑start).
















Known Limitations:
- GeoServer CQL does not support SELECT subqueries
- Cross-layer spatial queries must use queryCollection() function
- String literals in CQL must be properly quoted
"""
















from __future__ import annotations
















import asyncio
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional
















from mcp_server import MassGISCatalogServer  # local module in the Lambda layer
















# ---------------------------------------------------------------------------
#  Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
















# Emit deployed timestamp once per container so we can verify in logs
if ts := os.getenv("APP_TS"):
    logger.info("🚀 MassGIS MCP Lambda build %s loaded", ts)
















# ---------------------------------------------------------------------------
#  One global MassGIS server – loaded once per Lambda container (cold‑start)
# ---------------------------------------------------------------------------
GLOBAL_SERVER = MassGISCatalogServer()
















# ---------------------------------------------------------------------------
#  Helper – map tool names (camelCase keys)
# ---------------------------------------------------------------------------
TOOL_MAP: dict[str, Any] = {
    "searchLayers":     GLOBAL_SERVER._search_layers,
    "describeSchema":   GLOBAL_SERVER._describe_layer_schema,
    "querySpatial":     GLOBAL_SERVER._query_spatial,
    "findInTown":       GLOBAL_SERVER._intersect_with_town,
    "findNearby":       GLOBAL_SERVER._find_nearby,
    "listCategories":   GLOBAL_SERVER._list_categories,
    "categories":       GLOBAL_SERVER._list_categories,   # alias
    "getLayerDetails":  GLOBAL_SERVER._get_layer_details,
    "layerDetails":     GLOBAL_SERVER._get_layer_details,  # alias for /layer-details
    "massmapperLink":   GLOBAL_SERVER._generate_massmapper_link,
    "generateMapLink":  GLOBAL_SERVER._generate_massmapper_link,  # alias
}
















# ---------------------------------------------------------------------------
#  Helpers – argument extraction & normalisation
# ---------------------------------------------------------------------------
















def _kebab_or_snake_to_camel(name: str) -> str:
    """Convert kebab‑ or snake‑case to camelCase:  search-layers → searchLayers"""
    if not name:
        return ""
    tmp = re.sub(r"[_-]([a-z])", lambda m: m.group(1).upper(), name)
    return tmp[0].lower() + tmp[1:]
































def _parse_numeric(val: Any) -> Any:
    if isinstance(val, str):
        s = val.strip("\"'")
        # booleans as strings are super common from Bedrock tools
        if s.lower() == "true":
            return True
        if s.lower() == "false":
            return False
        # int or float (allow leading minus)
        if re.fullmatch(r"-?\d+(?:\.\d+)?", s):
            return int(s) if re.fullmatch(r"-?\d+", s) else float(s)
        return s
    return val
















def _maybe_json(v: Any) -> Any:
    if isinstance(v, str):
        s = v.strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except Exception:
                return v
    return v
































def _extract_kv_pairs(lst: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for p in lst or []:
        name = p.get("name")
        value = _maybe_json(p.get("value"))








        # Special handling for CQL parameter to fix quote escaping
        if name == "cql" and isinstance(value, str):
            # Fix escaped quotes that might come from the agent
            value = value.replace('\\"', '"')
            # Fix single quotes that are getting truncated
            if value.count("'") % 2 != 0:
                value += "'"








        out[name] = _parse_numeric(value)
    return out
































def _extract_from_request_body(rb: Dict[str, Any]) -> Dict[str, Any]:
    """Extract parameters from request body, handling different shapes"""
    if not isinstance(rb, dict):
        return {}
   
    content = rb.get("content", {})
    if not isinstance(content, dict):
        return {}
   
    js = content.get("application/json")
    if not js:
        return {}
   
    # Handle the nested 'properties' structure
    if isinstance(js, dict) and "properties" in js:
        properties = js["properties"]
        if isinstance(properties, list):
            return _extract_kv_pairs(properties)
        elif isinstance(properties, dict):
            return {k: _parse_numeric(_maybe_json(v)) for k, v in properties.items()}
   
    # Handle direct list
    elif isinstance(js, list):
        return _extract_kv_pairs(js)
   
    # Handle direct dict
    elif isinstance(js, dict):
        return {k: _parse_numeric(_maybe_json(v)) for k, v in js.items()}
   
    return {}
















# ---------------------------------------------------------------------------
#  Core async runner
# ---------------------------------------------------------------------------
async def _run_tool(tool_name: str, args: Dict[str, Any]) -> List[str]:
    coro = TOOL_MAP.get(tool_name)
    if not coro:
        return [f"❌ Unknown tool: {tool_name}"]
    logger.debug("Invoking %s with %s", tool_name, args)
    try:
        # Check if the server's session needs to be recreated
        if hasattr(GLOBAL_SERVER, 'session') and GLOBAL_SERVER.session:
            try:
                if hasattr(GLOBAL_SERVER.session, 'is_closed') and GLOBAL_SERVER.session.is_closed:
                    GLOBAL_SERVER.session = None
            except:
                # If any error checking session state, just reset it
                GLOBAL_SERVER.session = None
       
        result_objs = await coro(**args) if args else await coro()
        return [getattr(o, "text", str(o)) for o in result_objs]
    except Exception as exc:
        logger.exception("Tool %s failed", tool_name)
        # If it's an event loop error, try to reset the session
        if "Event loop is closed" in str(exc):
            if hasattr(GLOBAL_SERVER, 'session'):
                GLOBAL_SERVER.session = None
        return [f"❌ Tool {tool_name} failed – {exc}"]
















# ---------------------------------------------------------------------------
#  Lambda handler – supports *both* old (flat) and new (nested) payload shapes
# ---------------------------------------------------------------------------
















def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    logger.info("Incoming event slice → %s", json.dumps(event)[:600])
















    try:
        # ------------------------------------------------------------------
        # 1. Locate the action‑group invocation section (nested or flat)
        # ------------------------------------------------------------------
        ag_input: Dict[str, Any] = event.get("actionGroupInvocationInput") or {}
















        if not ag_input:
            inv_input = event.get("invocationInput")
            if isinstance(inv_input, list) and inv_input:
                ag_input = inv_input[0].get("actionGroupInvocationInput", {}) or {}
            elif isinstance(inv_input, dict):
                ag_input = inv_input.get("actionGroupInvocationInput", {}) or {}
















        # ag_input is **always** a dict from here on
        # ------------------------------------------------------------------
        raw_path = ag_input.get("apiPath", event.get("apiPath", ""))
        verb     = ag_input.get("verb",     event.get("httpMethod", "POST"))
        parameters   = ag_input.get("parameters",   event.get("parameters", []))
        request_body = ag_input.get("requestBody",  event.get("requestBody", {}))
















        # ------------------------------------------------------------------
        # 2. Normalise the tool key & arguments
        # ------------------------------------------------------------------
        tool_key = _kebab_or_snake_to_camel(raw_path.strip("/").split("/")[-1])
















        args: Dict[str, Any] = {}
        args.update(_extract_kv_pairs(parameters))
        args.update(_extract_from_request_body(request_body))
















        # Inject defaults so positional sigs are always satisfied
        if tool_key == "searchLayers":
            args.setdefault("query", "")  # Provide empty string default
            args.setdefault("category", None)
            args.setdefault("limit", 10)
       
        # Fix parameter name mismatches
        if tool_key == "describeSchema" and "layer_name" in args:
            args["lid"] = args.pop("layer_name")
       
        if tool_key in ["getLayerDetails", "layerDetails"] and "layer_name" in args:
            args["lid"] = args.pop("layer_name")
       
        if tool_key == "querySpatial" and "layer_name" in args:
            args["lid"] = args.pop("layer_name")
       
        if tool_key == "findInTown" and "layer_name" in args:
            args["layer_name"] = args.pop("layer_name")  # Keep as layer_name for this one
       
        if tool_key == "findNearby":
            # map layer_name → lid (server method param)
            if "layer_name" in args:
                args["lid"] = args.pop("layer_name")








            # latitude/longitude to lat/lon, with generous aliases
            if "latitude" in args:
                args["lat"] = args.pop("latitude")
            if "lat" not in args and "y" in args:
                args["lat"] = args.pop("y")








            if "longitude" in args:
                args["lon"] = args.pop("longitude")
            if "lng" in args:
                args["lon"] = args.pop("lng")
            if "lon" not in args and "x" in args:
                args["lon"] = args.pop("x")








            args.setdefault("radius_meters", 1000)
       
        # Add missing defaults for other tools
        if tool_key == "findInTown":
            args.setdefault("max_features", 50)
       
        if tool_key == "querySpatial":
            args.setdefault("max_features", 50)
            args.setdefault("start_index", 0)
            args.setdefault("sort_by", None)
            # Log CQL queries for debugging and normalize
            if "cql" in args and isinstance(args["cql"], str):
                cql = args["cql"]
                logger.info(f"Original CQL query: {cql}")








                # 1) close odd quotes
                if cql.count("'") % 2 != 0:
                    cql += "'"








                # 2) convert function-style ILIKE(field,'x') → field ILIKE 'x'
                cql = re.sub(
                    r"\bILIKE\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*'([^']*)'\s*\)",
                    r"\1 ILIKE '\2'",
                    cql,
                )








                # 3) quote bare identifiers on LHS of LIKE/ILIKE/=/IN
                def _q_ident(m):
                    ident = m.group(1)
                    return f'"{ident}" {m.group(2)} {m.group(3)}'
                cql = re.sub(
                    r"\b([A-Za-z_][A-Za-z0-9_]*)\b\s+(ILIKE|LIKE|=|IN)\s+(.*)",
                    _q_ident,
                    cql,
                    count=1,
                )








                # 4) warn on SELECT
                if "SELECT" in cql.upper():
                    logger.warning("CQL contains SELECT statement which is not supported by GeoServer")








                args["cql"] = cql
                logger.info(f"Normalized CQL: {args['cql']}")
       
        if tool_key in ["massmapperLink", "generateMapLink"]:
            args.setdefault("use_union_bbox", True)
            args.setdefault("bbox", None)
            args.setdefault("include_all_used", True)
            args.setdefault("specific_layers", None)
       
        logger.debug("Normalised args → %s", args)
















        # ------------------------------------------------------------------
        # 3. Run the tool (async) and build the Bedrock response payload
        # ------------------------------------------------------------------
        # Handle event loop issues in Lambda
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            result = loop.run_until_complete(_run_tool(tool_key, args))
        except RuntimeError:
            # If there's no event loop, create one
            result = asyncio.run(_run_tool(tool_key, args))
       
        body_json = json.dumps({"result": result}, ensure_ascii=False)
















        return {
            "messageVersion": "1.0",
            "response": {
                "actionGroup": event.get("actionGroup"),
                "apiPath": raw_path,
                "httpMethod": verb,
                "httpStatusCode": 200,
                "responseBody": {"application/json": {"body": body_json}},
            },
            "sessionAttributes": event.get("sessionAttributes", {}),
            "promptSessionAttributes": event.get("promptSessionAttributes", {}),
        }
















    except Exception as exc:
        logger.exception("Unhandled error in lambda_handler")
        err_body = json.dumps({"error": str(exc)}, ensure_ascii=False)
        return {
            "messageVersion": "1.0",
            "response": {
                "actionGroup": event.get("actionGroup"),
                "apiPath": event.get("apiPath"),
                "httpMethod": event.get("httpMethod", "POST"),
                "httpStatusCode": 500,
                "responseBody": {"application/json": {"body": err_body}},
            },
            "sessionAttributes": event.get("sessionAttributes", {}),
            "promptSessionAttributes": event.get("promptSessionAttributes", {}),
        }





