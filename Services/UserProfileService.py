# services/user_profile.py
import enum
from typing import Dict, Any

class RiskProfile(str, enum.Enum):
    CONSERVATIVE = "conservative"
    MODERATE = "moderate"
    AGGRESSIVE = "aggressive"

class ExperienceLevel(str, enum.Enum):
    NOVICE = "novice"
    INTERMEDIATE = "intermediate"
    EXPERT = "expert"

class UserProfileService:
    """
    Сервис для обработки ответов на опросник (цифры 1-4) и определения риск-профиля.
    """

    QUESTION_WEIGHTS = {
        'q1': 0.3,
        'q2': 0.3,
        'q3': 0.2,
        'q4': 0.2
    }

    def calculate_profile(self, answers: Dict[str, int]) -> Dict[str, Any]:
        """
        Принимает словарь с ключами q1..q4 и целыми числами от 1 до 4.
        Возвращает профиль и параметры для оптимизации портфеля.
        """
        # Взвешенная сумма ответов (сами ответы используем как баллы)
        risk_score = sum(self.QUESTION_WEIGHTS[q] * answers[q] for q in answers)

        # Определение риск-профиля
        if risk_score < 2.0:
            risk_profile = RiskProfile.CONSERVATIVE
        elif risk_score < 3.0:
            risk_profile = RiskProfile.MODERATE
        else:
            risk_profile = RiskProfile.AGGRESSIVE

        # Определение уровня опыта по вопросу 4
        q4 = answers['q4']
        if q4 <= 2:
            experience_level = ExperienceLevel.NOVICE
        elif q4 == 3:
            experience_level = ExperienceLevel.INTERMEDIATE
        else:  # q4 == 4
            experience_level = ExperienceLevel.EXPERT

        # Параметры для портфеля
        params = self._get_portfolio_parameters(risk_profile, experience_level)

        return {
            'risk_score': round(risk_score, 2),
            'risk_profile': risk_profile.value,
            'experience_level': experience_level.value,
            'parameters': params
        }

    def _get_portfolio_parameters(self, risk_profile: RiskProfile, experience_level: ExperienceLevel) -> Dict[str, Any]:
        params = {}

        # Безрисковая ставка в зависимости от профиля
        if risk_profile == RiskProfile.CONSERVATIVE:
            params['risk_free_rate'] = 0.07
        elif risk_profile == RiskProfile.MODERATE:
            params['risk_free_rate'] = 0.05
        else:  # aggressive
            params['risk_free_rate'] = 0.03

        # Ограничение на максимальный вес актива
        if risk_profile == RiskProfile.CONSERVATIVE:
            params['max_weight'] = 0.15
        elif risk_profile == RiskProfile.MODERATE:
            params['max_weight'] = 0.25
        else:  # aggressive
            params['max_weight'] = 0.40

        # Метод кластеризации для HRP в зависимости от опыта
        if experience_level == ExperienceLevel.NOVICE:
            params['linkage_method'] = 'ward'      # более устойчивый
        else:
            params['linkage_method'] = 'single'    # более гибкий, может давать концентрированные веса

        return params