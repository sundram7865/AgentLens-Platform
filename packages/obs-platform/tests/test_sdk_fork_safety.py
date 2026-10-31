"""The publisher must survive fork() without blocking the child's request path.

fork() copies one thread. If the parent's drain thread held the queue's lock at
that instant, the child inherits a lock no thread will ever release, and its
first publish() blocks forever. gunicorn --preload and Celery prefork both fork
after the application has imported the SDK, so this is not hypothetical.

POSIX only: Windows has no fork, and CI runs this on Linux.
"""

from __future__ import annotations

import os
import sys
import warnings

import pytest

from obs_sdk.publisher import RedisPublisherConfig, RedisStreamPublisher
from obs_sdk.schema import EventType, ObsEvent

pytestmark = pytest.mark.skipif(not hasattr(os, "fork"), reason="fork() is POSIX-only")


class _NoRedis:
    """A client that accepts pipelines and does nothing, so no socket is needed."""

    def ping(self) -> bool:
        return True

    def pipeline(self, transaction: bool = False) -> _NoRedis:
        return self

    def xadd(self, *args: object, **kwargs: object) -> None:
        return None

    def execute(self) -> list[object]:
        return []

    def close(self) -> None:
        return None


def test_a_child_forked_while_the_queue_lock_is_held_can_still_publish() -> None:
    publisher = RedisStreamPublisher(RedisPublisherConfig(url="redis://unused"), client=_NoRedis())
    event = ObsEvent(trace_id="tr_fork", type=EventType.SPAN_END)
    read_fd, write_fd = os.pipe()
    try:
        with warnings.catch_warnings():
            # 3.12+ warns that forking a multi-threaded process can deadlock.
            # That deadlock is exactly what this test proves does not happen.
            warnings.simplefilter("ignore", DeprecationWarning)
            # Fork at the worst moment: with the queue's mutex held, as if the
            # drain thread were mid-get().
            with publisher._queue.mutex:
                pid = os.fork()
        if pid == 0:  # child
            try:
                import signal

                signal.alarm(5)  # a deadlock fails the test instead of hanging it
                ok = publisher.publish(event) and publisher.flush(timeout=3)
                alive = publisher._thread.is_alive()
                os.write(write_fd, b"ok" if ok and alive else b"bad")
            except BaseException as exc:
                os.write(write_fd, f"err {exc!r}".encode())
            finally:
                os._exit(0)

        os.close(write_fd)
        write_fd = -1
        _, status = os.waitpid(pid, 0)
        result = os.read(read_fd, 200).decode()
        assert os.WIFEXITED(status), "child was killed (deadlocked until SIGALRM)"
        assert result == "ok", result
        # The parent is untouched by the child's reinitialisation.
        assert publisher.publish(event) is True
    finally:
        os.close(read_fd)
        if write_fd != -1:
            os.close(write_fd)
        publisher.close(timeout=1)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX-only")
def test_a_closed_publisher_stays_closed_in_the_child() -> None:
    publisher = RedisStreamPublisher(RedisPublisherConfig(url="redis://unused"), client=_NoRedis())
    publisher.close(timeout=1)
    publisher._reinit_after_fork()  # what the at-fork hook would call
    assert not publisher._thread.is_alive()
