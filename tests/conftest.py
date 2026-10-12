import os
import sqlite3

import pytest

from pipelineforge.generate import generate


@pytest.fixture
def paths(tmp_path):
    return str(tmp_path / "oltp.db"), str(tmp_path / "wh.db")


@pytest.fixture
def clean_src(paths):
    """Small, fully clean source database."""
    info = generate(paths[0], customers=30, products=10, orders=100, dirty_rate=0.0, seed=3)
    return paths, info


def wh_rows(path, sql, params=()):
    c = sqlite3.connect(path)
    try:
        return c.execute(sql, params).fetchall()
    finally:
        c.close()
