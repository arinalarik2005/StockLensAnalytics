# orchestrator.py
from schemas import OptimizeRequest, OptimizeResponse

from Services.PortfolioService import PortfolioService
from Services.UserProfileService import UserProfileService
class PortfolioOrchestrator:
    def __init__(self):
        self.user_profile_service = UserProfileService()
        self.portfolio_service = PortfolioService()

    def process(self, request: OptimizeRequest) -> OptimizeResponse:
        # 1. Подготовка ответов на опросник
        answers = {
            'q1': request.m1,
            'q2': request.m2,
            'q3': request.m3,
            'q4': request.m4
        }

        # 2. Получение профиля и параметров
        profile_data = self.user_profile_service.calculate_profile(answers)
        params = profile_data['parameters']

        # 3. Преобразование котировок
        quotes_dicts = [item.dict() for item in request.quotes]

        # 4. Оптимизация портфеля
        portfolio_result = self.portfolio_service.optimize(
            data=quotes_dicts,
            risk_free_rate=params['risk_free_rate'],
            linkage_method=params['linkage_method'],
            max_weight=params.get('max_weight')
        )

        # 5. Формирование ответа
        return OptimizeResponse(
            weights=portfolio_result['weights'],
            expected_return=portfolio_result['expected_return'],
            volatility=portfolio_result['volatility'],
            sharpe_ratio=portfolio_result['sharpe_ratio'],
            risk_profile=profile_data['risk_profile'],
            experience_level=profile_data['experience_level']
        )