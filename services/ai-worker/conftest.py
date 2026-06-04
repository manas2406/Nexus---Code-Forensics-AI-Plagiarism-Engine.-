import pytest

def pytest_configure(config):
    config.addinivalue_line("markers", "llm: mark test as requiring real OpenAI API key")
