import logging

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import override

from rich.progress import Progress
from rich.console import Console


class ProgressReporter(ABC):
    @abstractmethod
    def start(self):
        raise NotImplementedError()

    @abstractmethod
    def stop(self):
        raise NotImplementedError()

    @abstractmethod
    def add_task(self, description: str, total=None) -> int:
        raise NotImplementedError()

    @abstractmethod
    def advance(self, task_id: int, advance: int = 1) -> None:
        raise NotImplementedError()

    @abstractmethod
    def remove_task(self, task_id: int):
        raise NotImplementedError()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()


class RichProgressReporter(ProgressReporter):
    def __init__(self, progress: Progress | None = None, **kwargs):
        super().__init__()
        if progress is None:
            progress = Progress(**kwargs)

        self.progress = progress

    @override
    def start(self):
        self.progress.start()

    @override
    def stop(self):
        self.progress.stop()

    @override
    def add_task(self, description, total=None):
        return self.progress.add_task(description=description, total=total)

    @override
    def advance(self, task_id, advance: int = 1):
        self.progress.advance(task_id, advance)

    @override
    def remove_task(self, task_id):
        return self.progress.remove_task(task_id)


class LoggingProgressReporter(ProgressReporter):
    @dataclass
    class Task:
        task_id: int
        description: str
        total: int | None
        completed: int = 0

        def __str__(self) -> str:
            if self.total is not None:
                msg = f"[{self.description}] [f{'#' * int(10 * self.completed / self.total):-<10}] [{self.completed} / {self.total}]"
            else:
                msg = f"[{self.description}] [{self.completed}]"

            return msg

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.task_id = 0
        self.tasks: dict[int, LoggingProgressReporter.Task] = dict()

    @override
    def start(self):
        self.logger.info("start progress report")

    @override
    def stop(self):
        self.logger.info("stop progress report")

    @override
    def add_task(self, description, total=None):
        task_id = self.task_id
        self.task_id += 1
        self.logger.info(f"adding task {description} with id {self.task_id}")
        task = LoggingProgressReporter.Task(task_id, description, total)
        self.tasks[task_id] = task
        return task_id

    @override
    def advance(self, task_id, advance: int = 1):
        if task_id not in self.tasks:
            raise KeyError(f"no task with id {task_id}")

        task = self.tasks[task_id]
        task.completed += advance

        self.logger.info(task)

    @override
    def remove_task(self, task_id):
        if task_id not in self.tasks:
            raise KeyError(f"no task with id {task_id}")

        task = self.tasks.pop(task_id)
        self.logger.info(f"removing task {task.description}")


class NullProgressReporter(ProgressReporter):
    @override
    def start(self):
        pass

    @override
    def stop(self):
        pass

    @override
    def add_task(self, description, total=None):
        return 0

    @override
    def advance(self, task_id, advance: int = 1):
        pass

    @override
    def remove_task(self, task_id):
        pass
