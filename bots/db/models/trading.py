from . import db

class KlineData(db.Model):
    __tablename__ = 'kline_data'

    id = db.Column(db.Integer, primary_key=True)
    open_time = db.Column(db.DateTime, nullable=False)
    close_time = db.Column(db.DateTime, nullable=False)
    symbol = db.Column(db.String(16), nullable=False)
    interval = db.Column(db.String(8), nullable=False)
    open_price = db.Column(db.Float, nullable=False)
    close_price = db.Column(db.Float, nullable=False)
    high_price = db.Column(db.Float, nullable=False)
    low_price = db.Column(db.Float, nullable=False)
    volume = db.Column(db.Float, nullable=False)
    quote_volume = db.Column(db.Float, nullable=False)
    trades = db.Column(db.Integer, nullable=False)
    taker_buy_base_asset_volume = db.Column(db.Float, nullable=False)
    taker_buy_quote_asset_volume = db.Column(db.Float, nullable=False)
    ignore = db.Column(db.Float, nullable=False)

    market = db.Column(db.String(16), nullable=False, default="unknown")  # Add this line for market type