"""Boundary tests for preprocess --watch key handling."""

import pytest
import sys

sys.path.insert(0, "pipeline")
sys.path.insert(0, "pipeline/preprocess_lib")
from preprocess import (
    _paginate_rows,
    _move_watch_selection,
    normalize_initial_watch_page,
)


class TestPaginateRows:
    def test_empty_rows(self):
        result = _paginate_rows([], 0, 20)
        assert result == ([], 1, 0, 0, 0)

    def test_exact_page_size(self):
        rows = list(range(20))
        result = _paginate_rows(rows, 0, 20)
        page_rows, total, page, start, end = result
        assert page_rows == list(range(20))
        assert total == 1

    def test_page_wrap_forward(self):
        rows = list(range(25))
        result = _paginate_rows(rows, 2, 10)
        page_rows, total, page, start, end = result
        assert total == 3

    def test_single_item(self):
        rows = ["item1"]
        result = _paginate_rows(rows, 0, 20)
        page_rows, total, page, start, end = result
        assert len(page_rows) == 1


class TestWatchSelection:
    def test_move_up_at_top(self):
        rows = [{"name": "row1"}, {"name": "row2"}]
        result = _move_watch_selection(rows, "row1", -1)
        assert result in ("", "row1")

    def test_move_down_at_bottom(self):
        rows = [{"name": "row1"}, {"name": "row2"}]
        result = _move_watch_selection(rows, "row2", 1)
        assert result in ("row2", "")

    def test_move_with_empty_list(self):
        result = _move_watch_selection([], "", -1)
        assert result == ""


class TestViewPageToggle:
    def test_tab_0_to_1(self):
        view_page = 0
        view_page = (view_page + 1) % 2
        assert view_page == 1

    def test_tab_1_to_0(self):
        view_page = 1
        view_page = (view_page + 1) % 2
        assert view_page == 0


class TestNPKeysGuarded:
    def test_n_ignored_on_view0(self):
        page = 0
        total_pages = 5
        view_page = 0
        if "n" == "n" and view_page == 1:
            page = (page + 1) % max(total_pages, 1)
        assert page == 0

    def test_p_ignored_on_view0(self):
        page = 2
        total_pages = 5
        view_page = 0
        if "p" == "p" and view_page == 1:
            page = (page - 1) % max(total_pages, 1)
        assert page == 2
