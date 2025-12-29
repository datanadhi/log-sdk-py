"""Thread-safe queue with batch operations and overflow handling."""

import threading
from queue import Empty, Full, Queue


class SafeQueue:
    """Thread-safe queue with batch operations and writeback buffer.

    Features:
    - get() returns None instead of raising Empty
    - get_batch(n) retrieves up to n items
    - writeback_batch(items) can temporarily expand beyond maxsize
    - Tracks fill percentage for overflow detection
    """

    def __init__(self, maxsize: int):
        self.maxsize = maxsize
        self._queue = Queue(maxsize=maxsize)
        self._lock = threading.Lock()
        self._writeback_buffer = []  # Temporary buffer for failed items

    def add(self, item: tuple) -> bool:
        """Add item to queue. Returns True if successful, False if full."""
        try:
            self._queue.put_nowait(item)
            return True
        except Full:
            return False

    def _get(self, timeout: float = 1.0) -> tuple | None:
        """Get one item from queue. Returns None if empty."""
        try:
            return self._queue.get(block=True, timeout=timeout)
        except Empty:
            return None

    def get(self, timeout: float = 1.0) -> tuple | None:
        """Get one item, attempting writeback drain first."""
        item = self._get(timeout=timeout)
        self._try_drain_writeback()
        if item is None:
            return self._get(timeout=timeout)
        return item

    def _try_drain_writeback(self):
        """Try draining writeback buffer to queue without blocking."""
        with self._lock:
            if not self._writeback_buffer:
                return

            # local copy prevents holding the lock while pushing to queue
            buf = self._writeback_buffer
            self._writeback_buffer = []

        for item in buf:
            try:
                self._queue.put_nowait(item)
            except Full:
                # queue became full again → put rest back
                with self._lock:
                    self._writeback_buffer.append(item)
                break

    def get_batch(self, n: int) -> list[tuple]:
        """Get up to n items from queue without blocking."""
        items = []
        for _ in range(n):
            try:
                items.append(self._queue.get_nowait())
            except Empty:
                break
        return items

    def writeback_batch(self, items: list[tuple]) -> int:
        """Write back items to queue or buffer. Returns count written."""
        written = 0

        # First try to write from buffer if any
        with self._lock:
            if self._writeback_buffer:
                buffer_copy = self._writeback_buffer.copy()
                self._writeback_buffer.clear()

                for item in buffer_copy:
                    try:
                        self._queue.put_nowait(item)
                        written += 1
                    except Full:
                        self._writeback_buffer.append(item)
                        break

        # Now try to write back new items
        for item in items:
            try:
                self._queue.put_nowait(item)
                written += 1
            except Full:
                # Store in buffer for later
                with self._lock:
                    self._writeback_buffer.append(item)

        return written

    def task_done(self):
        """Mark task as done."""
        self._queue.task_done()

    def join(self):
        """Block until all tasks are done."""
        self._queue.join()

    def qsize(self) -> int:
        """Get approximate queue size."""
        return self._queue.qsize()

    def empty(self) -> bool:
        """Check if queue is empty."""
        return self._queue.empty() and not self._writeback_buffer

    def fill_percentage(self) -> float:
        """Get fill percentage (0.0 to 1.0+). Includes writeback buffer."""
        queue_size = self._queue.qsize()
        buffer_size = len(self._writeback_buffer)
        total = queue_size + buffer_size
        return total / self.maxsize if self.maxsize > 0 else 0.0

    @property
    def unfinished_tasks(self) -> int:
        """Get number of unfinished tasks."""
        return self._queue.unfinished_tasks
