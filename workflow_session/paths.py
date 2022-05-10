def get_session_directory(session_key: dict) -> str:
    from .pipeline import session

    session_dir = (session.SessionDirectory & session_key).fetch1("session_dir")
    return session_dir
