"""Data schemas and validation for the Liquipedia poller"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict
from exceptions import DataValidationError

@dataclass
class Team:
    team_id: Optional[int]
    team_name: str
    region: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def validate(self) -> bool:
        """Validate team data"""
        if not self.team_name or len(self.team_name.strip()) == 0:
            raise DataValidationError("Team name cannot be empty")
        if not self.region or len(self.region.strip()) == 0:
            raise DataValidationError("Region cannot be empty")
        return True

@dataclass
class Tournament:
    tournament_id: Optional[int]
    name: str
    date: datetime
    region: str
    status: str = 'upcoming'
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def validate(self) -> bool:
        """Validate tournament data"""
        if not self.name or len(self.name.strip()) == 0:
            raise DataValidationError("Tournament name cannot be empty")
        if not self.region or len(self.region.strip()) == 0:
            raise DataValidationError("Region cannot be empty")
        if self.status not in ['upcoming', 'ongoing', 'completed']:
            raise DataValidationError("Invalid tournament status")
        if not isinstance(self.date, datetime):
            raise DataValidationError("Invalid date format")
        return True

@dataclass
class Match:
    match_id: Optional[int]
    tournament_id: int
    team1_id: int
    team2_id: int
    winner_id: Optional[int]
    score: str
    stage: str
    match_date: datetime
    status: str = 'scheduled'
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def validate(self) -> bool:
        """Validate match data"""
        if not all([self.tournament_id, self.team1_id, self.team2_id]):
            raise DataValidationError("Tournament and team IDs are required")
        if self.team1_id == self.team2_id:
            raise DataValidationError("Team 1 and Team 2 cannot be the same")
        if self.winner_id and self.winner_id not in [self.team1_id, self.team2_id]:
            raise DataValidationError("Winner must be one of the participating teams")
        if self.status not in ['scheduled', 'ongoing', 'completed', 'cancelled']:
            raise DataValidationError("Invalid match status")
        if not isinstance(self.match_date, datetime):
            raise DataValidationError("Invalid match date format")
        return True

    def to_dict(self) -> Dict:
        """Convert match to dictionary for JSON serialization"""
        return {
            'match_id': self.match_id,
            'tournament_id': self.tournament_id,
            'team1_id': self.team1_id,
            'team2_id': self.team2_id,
            'winner_id': self.winner_id,
            'score': self.score,
            'stage': self.stage,
            'match_date': self.match_date.isoformat() if self.match_date else None,
            'status': self.status
        }

@dataclass
class MatchDependency:
    source_match_id: int
    target_match_id: int
    position: int
    dependency_type: str
    created_at: Optional[datetime] = None

    def validate(self) -> bool:
        """Validate dependency data"""
        if self.source_match_id == self.target_match_id:
            raise DataValidationError("Source and target match cannot be the same")
        if self.position not in [1, 2]:
            raise DataValidationError("Position must be 1 or 2")
        if self.dependency_type not in ['winner', 'loser']:
            raise DataValidationError("Invalid dependency type")
        return True

    def to_dict(self) -> Dict:
        """Convert dependency to dictionary for JSON serialization"""
        return {
            'source_match_id': self.source_match_id,
            'target_match_id': self.target_match_id,
            'position': self.position,
            'dependency_type': self.dependency_type
        }