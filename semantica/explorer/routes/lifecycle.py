"""Authenticated private lifecycle routes for KNX's Semantica adapter."""

from __future__ import annotations

import re

from fastapi import APIRouter, Body, Depends, HTTPException, Path

from ..dependencies import get_session
from ..lifecycle import LifecycleService, _artifact_id
from ..schemas import (
    LifecycleEnumerationResponse,
    LifecycleNodePurgeRequest,
    LifecyclePurgeRequest,
    LifecyclePurgeResponse,
    LifecycleScopeRequest,
    LifecycleVerificationResponse,
)
from ..session import GraphSession

router = APIRouter(prefix="/api/lifecycle", tags=["Private Lifecycle"])
_CONTROL_CHARACTERS = re.compile(r"[\x00-\x1f\x7f]")


def _validated_reason(reason: str) -> str:
    value = reason.strip()
    if not value or _CONTROL_CHARACTERS.search(value):
        raise HTTPException(status_code=422, detail="Invalid lifecycle reason")
    return value


def _response_status(checks: dict) -> str:
    return (
        "residual"
        if any(check.get("status") == "residual" for check in checks.values())
        else "complete"
    )


@router.post(
    "/subjects/enumerate",
    response_model=LifecycleEnumerationResponse,
)
async def enumerate_subject(
    body: LifecycleScopeRequest,
    session: GraphSession = Depends(get_session),
):
    service = LifecycleService(session)
    nodes = service.scoped_nodes(body.tenant_id, body.subject_id)
    return LifecycleEnumerationResponse(
        tenant_id=body.tenant_id,
        subject_id=body.subject_id,
        node_ids=[str(node["id"]) for node in nodes],
        artifact_ids=sorted(
            artifact_id
            for node in nodes
            if (artifact_id := _artifact_id(node)) is not None
        ),
    )


@router.post(
    "/subjects/verify",
    response_model=LifecycleVerificationResponse,
)
async def verify_subject(
    body: LifecycleScopeRequest,
    session: GraphSession = Depends(get_session),
):
    checks = LifecycleService(session).verify(
        body.tenant_id,
        body.subject_id,
        node_ids=body.node_ids,
        artifact_ids=body.artifact_ids,
    )
    return LifecycleVerificationResponse(
        status=_response_status(checks),
        checks=checks,
    )


@router.post(
    "/subjects/purge",
    response_model=LifecyclePurgeResponse,
)
async def purge_subject(
    body: LifecyclePurgeRequest,
    session: GraphSession = Depends(get_session),
):
    purged, artifact_ids, checks = LifecycleService(session).purge_subject(
        body.tenant_id,
        body.subject_id,
        reason=_validated_reason(body.reason),
    )
    return LifecyclePurgeResponse(
        status=_response_status(checks),
        purged_count=len(purged),
        purged_node_ids=purged,
        artifact_ids=artifact_ids,
        checks=checks,
    )


@router.delete(
    "/nodes/{node_id}",
    response_model=LifecyclePurgeResponse,
)
async def purge_node(
    node_id: str = Path(min_length=1, max_length=512),
    body: LifecycleNodePurgeRequest = Body(...),
    session: GraphSession = Depends(get_session),
):
    try:
        purged, artifact_ids, checks = LifecycleService(session).purge_node(
            node_id,
            body.tenant_id,
            body.subject_id,
            reason=_validated_reason(body.reason),
            cascade=body.cascade,
        )
    except KeyError:
        raise HTTPException(
            status_code=404, detail="Lifecycle artifact not found"
        ) from None
    return LifecyclePurgeResponse(
        status=_response_status(checks),
        purged_count=len(purged),
        purged_node_ids=purged,
        artifact_ids=artifact_ids,
        checks=checks,
    )
