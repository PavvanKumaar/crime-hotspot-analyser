"""
Validates grid-cell assignment against a few hand-calculated coordinates.

Run with:
    pytest tests/test_spatial_grid.py -v

No Spark session required -- compute_cell() is pure Python, so this runs
in well under a second and is a cheap check to run before every pipeline
run.
"""

import math
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.spatial_grid import compute_cell

CELL_SIZE = 0.01  # matches config.GRID_CELL_SIZE_DEGREES default


def test_origin_cell():
    # A point just inside the (0,0) cell
    cell_x, cell_y, center_lat, center_lon = compute_cell(0.005, 0.005, CELL_SIZE)
    assert cell_x == 0
    assert cell_y == 0
    assert math.isclose(center_lat, 0.005)
    assert math.isclose(center_lon, 0.005)


def test_known_chicago_point():
    # Approx Chicago Loop coordinates
    lat, lon = 41.8781, -87.6298
    cell_x, cell_y, center_lat, center_lon = compute_cell(lat, lon, CELL_SIZE)

    expected_cell_x = math.floor(lon / CELL_SIZE)   # floor(-87.6298 / 0.01) = -8763
    expected_cell_y = math.floor(lat / CELL_SIZE)   # floor(41.8781 / 0.01)  = 4187

    assert cell_x == expected_cell_x == -8763
    assert cell_y == expected_cell_y == 4187
    # The center of a cell should be within half a cell width of the point
    assert abs(center_lat - lat) <= CELL_SIZE
    assert abs(center_lon - lon) <= CELL_SIZE


def test_negative_coordinates_do_not_crash():
    cell_x, cell_y, center_lat, center_lon = compute_cell(-33.87, 151.21, CELL_SIZE)  # Sydney
    assert isinstance(cell_x, int)
    assert isinstance(cell_y, int)


def test_cell_boundary_rounds_down_not_toward_zero():
    # Negative longitude near a cell boundary: floor must round DOWN
    # (more negative), not toward zero, or western-hemisphere points get
    # silently misassigned by one cell.
    cell_x, _, _, _ = compute_cell(0.0, -0.001, CELL_SIZE)
    assert cell_x == -1  # NOT 0


def test_adjacent_points_can_share_a_cell():
    a = compute_cell(41.8781, -87.6298, CELL_SIZE)
    b = compute_cell(41.8782, -87.6299, CELL_SIZE)  # a few meters away
    assert (a[0], a[1]) == (b[0], b[1])
