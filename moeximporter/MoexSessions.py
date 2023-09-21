from enum import IntEnum

class MoexSessions(IntEnum):
    """Enum for MOEX sessions. Not applicable to all instruments.
    """

    MorningSession = 0,
    """Morning trading session.
    """
    MainSession = 1,
    """Main trading session.
    """
    EveningSession = 2,
    """Evening tradning session.
    """
    TotalSessions = 3
    """Data for all sessions.
    """