# -*- coding: utf-8 -*-
"""Tests for the DPAPI secret helpers."""
import os

import pytest

from pdi2dag import dpapi

WINDOWS = os.name == 'nt'


def test_empty_and_plaintext_passthrough():
    assert dpapi.protect('') == ''
    assert dpapi.protect(None) is None
    assert dpapi.unprotect('') == ''
    assert dpapi.unprotect('plaintext-secret') == 'plaintext-secret'


def test_is_protected():
    assert dpapi.is_protected('dpapi:abc')
    assert not dpapi.is_protected('abc')
    assert not dpapi.is_protected('')
    assert not dpapi.is_protected(None)


def test_protect_is_idempotent_marker():
    # Already-protected values are not double-wrapped.
    already = 'dpapi:already'
    assert dpapi.protect(already) == already


@pytest.mark.skipif(not WINDOWS, reason='DPAPI is Windows-only')
def test_roundtrip_windows():
    secret = 'S3cr3t-p@ss w0rd'
    enc = dpapi.protect(secret)
    assert enc.startswith(dpapi.PREFIX)
    assert enc != secret
    assert dpapi.unprotect(enc) == secret


@pytest.mark.skipif(WINDOWS, reason='non-Windows fallback')
def test_protect_noop_off_windows():
    # Off Windows, protect is a no-op (plaintext fallback).
    assert dpapi.protect('secret') == 'secret'
