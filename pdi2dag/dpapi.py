# -*- coding: utf-8 -*-
# Copyright 2026 Pentaho
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Windows DPAPI helpers to keep secrets out of ``settings.json`` in
plaintext.

Protected values are stored as ``dpapi:<base64>`` and are scoped to the
current Windows user + machine (``CryptProtectData``). On non-Windows -
or if DPAPI is unavailable - values pass through unchanged, so the app
still works with a plaintext fallback (the Studio runs on Windows, where
protection is active).
"""
from __future__ import annotations

import base64
import os

PREFIX = 'dpapi:'


def _on_windows():
    return os.name == 'nt'


def is_protected(value):
    return isinstance(value, str) and value.startswith(PREFIX)


# ---- ctypes DPAPI bindings (Windows only) ------------------------------

def _crypt(data, protect):
    import ctypes
    from ctypes import wintypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [('cbData', wintypes.DWORD),
                    ('pbData', ctypes.POINTER(ctypes.c_char))]

    src = ctypes.create_string_buffer(data, len(data))
    blob_in = DATA_BLOB(len(data),
                        ctypes.cast(src, ctypes.POINTER(ctypes.c_char)))
    blob_out = DATA_BLOB()
    fn = (ctypes.windll.crypt32.CryptProtectData if protect
          else ctypes.windll.crypt32.CryptUnprotectData)
    # (data, description, entropy, reserved, prompt, flags, out)
    ok = fn(ctypes.byref(blob_in), None, None, None, None, 0,
            ctypes.byref(blob_out))
    if not ok:
        raise ctypes.WinError()
    try:
        return ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(blob_out.pbData)


def protect(value):
    """Encrypt a plaintext string for storage. Returns the value unchanged
    when it is empty, already protected, or DPAPI is unavailable."""
    if not value or is_protected(value) or not _on_windows():
        return value
    try:
        enc = _crypt(value.encode('utf-8'), protect=True)
        return PREFIX + base64.b64encode(enc).decode('ascii')
    except OSError:
        return value


def unprotect(value):
    """Decrypt a stored string. Non-protected values are returned as-is;
    a protected value that cannot be decrypted (wrong user/machine, or
    off-Windows) is returned unchanged."""
    if not is_protected(value):
        return value
    if not _on_windows():
        return value
    try:
        raw = base64.b64decode(value[len(PREFIX):])
        return _crypt(raw, protect=False).decode('utf-8')
    except (OSError, ValueError):
        return value
