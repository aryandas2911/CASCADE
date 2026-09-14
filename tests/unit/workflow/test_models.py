from cascade.workflow.models import Task, Workflow


def test_task_creation() -> None:
    task = Task(id="test_task", dependencies=[])
    assert task.id == "test_task"
    assert task.dependencies == []


def test_task_with_dependencies() -> None:
    task = Task(
        id="task_2",
        dependencies=["test_task"],
    )

    assert task.id == "task_2"
    assert task.dependencies == ["test_task"]


def test_workflow_creation() -> None:
    workflow = Workflow(
        id="workflow_test",
        name="Test Workflow",
        tasks=[
            Task(id="test_task"),
            Task(id="task_2", dependencies=["test_task"]),
        ],
    )

    assert workflow.id == "workflow_test"
    assert workflow.name == "Test Workflow"
    assert len(workflow.tasks) == 2
