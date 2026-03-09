import os
from dotenv import load_dotenv
from utils.get_token import get_battle_net_access_token
from utils.get_league_data import get_league_data_raw

load_dotenv()

acesso = get_battle_net_access_token(
    client_id=os.getenv('BLIZZARD_CLIENT_ID'),
    client_secret=os.getenv('BLIZZARD_CLIENT_SECRET')
)

abc = get_league_data_raw(season_id=65, queue_id=201, team_type=0, league_id=4, token=acesso)
