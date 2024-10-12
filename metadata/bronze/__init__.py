"""
Import all the SCHEMA when you import metadata
"""

from . import (
    name_basics,
    title_akas,
    title_basics,
    title_crew,
    title_episode,
    title_principals,
    title_ratings,
)

# from .name_basics import *
# from .title_akas import *
# from .title_basics import *
# from .title_crew import *
# from .title_episode import *
# from .title_principals import *
# from .title_ratings import *

__all__ = [
    "name_basics",
    "title_akas",
    "title_basics",
    "title_crew",
    "title_episode",
    "title_principals",
    "title_ratings",
]
