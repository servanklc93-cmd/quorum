"""The deterministic simulation as part of the normal test run.

CI runs a few hundred seeds; `python -m quorum.sim --seeds 10000` runs as
many as you like. Every seed picks its own cluster size, features and fault
profile (swarm testing), so these seeds cover very different situations.
"""

import pytest

from quorum.sim import Simulation, Violation


@pytest.mark.parametrize("block", range(10))
def test_seeds(block):
    for seed in range(block * 30, block * 30 + 30):
        try:
            Simulation(seed).run()
        except Violation as e:
            raise AssertionError(f"seed {seed}: {e}\nreproduce: python -m quorum.sim --start {seed} --seeds 1") from None


def test_same_seed_same_run():
    a, b = Simulation(123).run(), Simulation(123).run()
    assert a == b


def test_the_runs_do_real_work():
    total = sum(Simulation(seed).run().completed for seed in range(20))
    assert total > 5000
