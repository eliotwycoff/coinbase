use crate::advanced::{
    common::Error,
    rest::{Client, DOMAIN},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smartstring::{LazyCompact, SmartString};
use time::OffsetDateTime;
use uuid::Uuid;

pub trait Orders {
    fn cancel_orders(
        &self,
        order_ids: &CancelOrderList,
    ) -> impl Future<Output = Result<CancelOrderResults, Error>>;
    fn create_order(
        &self,
        create_order: &CreateOrder,
    ) -> impl Future<Output = Result<CreatedOrder, Error>>;
    fn list_orders(&self) -> impl Future<Output = Result<OrderList, Error>>;
}

impl Orders for Client {
    async fn cancel_orders(
        &self,
        order_ids: &CancelOrderList,
    ) -> Result<CancelOrderResults, Error> {
        let path = "/api/v3/brokerage/orders/batch_cancel";
        let token = self.get_jwt("POST", DOMAIN, path)?;
        let body = serde_json::to_vec(order_ids)
            .map_err(|error| Error::invalid("order ids").with_source(Box::new(error)))?;

        self.get_response(|client| {
            client
                .post(format!("https://{DOMAIN}{path}"))
                .header("Authorization", format!("Bearer {token}"))
                .header("User-Agent", "RustSdk/0.1.0")
                .body(body)
        })
        .await
    }

    async fn create_order(&self, create_order: &CreateOrder) -> Result<CreatedOrder, Error> {
        let path = "/api/v3/brokerage/orders";
        let token = self.get_jwt("POST", DOMAIN, path)?;
        let body = serde_json::to_vec(create_order)
            .map_err(|error| Error::invalid("create order").with_source(Box::new(error)))?;

        self.get_response(|client| {
            client
                .post(format!("https://{DOMAIN}{path}"))
                .header("Authorization", format!("Bearer {token}"))
                .header("User-Agent", "RustSdk/0.1.0")
                .body(body)
        })
        .await
    }

    async fn list_orders(&self) -> Result<OrderList, Error> {
        let path = "/api/v3/brokerage/orders/historical/batch";
        let token = self.get_jwt("GET", DOMAIN, path)?;

        self.get_response(|client| {
            client
                .get(format!("https://{DOMAIN}{path}"))
                .header("Authorization", format!("Bearer {token}"))
                .header("User-Agent", "RustSdk/0.1.0")
                .query(&[("order_status", "OPEN")])
        })
        .await
    }
}

#[derive(Debug, Serialize)]
pub struct CancelOrderList {
    pub order_ids: Vec<Uuid>,
}

impl From<Vec<Uuid>> for CancelOrderList {
    fn from(order_ids: Vec<Uuid>) -> Self {
        Self { order_ids }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelOrderResult {
    failure_reason: SmartString<LazyCompact>,
    order_id: Uuid,
    success: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelOrderResults {
    results: Vec<CancelOrderResult>,
}

#[derive(Debug, Serialize)]
pub struct CreateOrder {
    client_order_id: Uuid,
    product_id: SmartString<LazyCompact>,
    side: Side,
    order_configuration: CreateOrderConfiguration,
}

impl CreateOrder {
    pub fn new(
        product_id: &str,
        side: Side,
        order_configuration: CreateOrderConfiguration,
    ) -> Self {
        Self {
            client_order_id: Uuid::new_v4(),
            product_id: product_id.into(),
            side,
            order_configuration,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CreateOrderConfiguration {
    LimitLimitGtc {
        base_size: Decimal,
        limit_price: Decimal,
        post_only: bool,
    },
}

impl CreateOrderConfiguration {
    pub fn limit_limit_gtc(base_size: Decimal, limit_price: Decimal, post_only: bool) -> Self {
        Self::LimitLimitGtc {
            base_size,
            limit_price,
            post_only,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreatedOrder {
    pub order_configuration: ListOrderConfiguration,
    pub success: bool,
    #[serde(alias = "success_response", alias = "error_response")]
    pub response: CreatedOrderResponse,
}

impl CreatedOrder {
    pub fn order_id(&self) -> Option<Uuid> {
        match self.response {
            CreatedOrderResponse::Success { order_id, .. } => Some(order_id),
            CreatedOrderResponse::Error { .. } => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CreatedOrderResponse {
    Success {
        attached_order_id: SmartString<LazyCompact>,
        client_order_id: Uuid,
        order_id: Uuid,
        product_id: SmartString<LazyCompact>,
        side: Side,
    },
    Error {
        message: SmartString<LazyCompact>,
        error_details: SmartString<LazyCompact>,
        preview_failure_reason: Option<CreatedOrderPreviewFailureReason>,
        new_order_failure_reason: Option<CreatedOrderNewOrderFailureReason>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CreatedOrderPreviewFailureReason {
    UnknownPreviewFailureReason,
    PreviewMissingCommissionRate,
    PreviewInvalidSide,
    PreviewInvalidOrderConfig,
    PreviewInvalidProductId,
    PreviewInvalidSizePrecision,
    PreviewInvalidPricePrecision,
    PreviewMissingProductPriceBook,
    PreviewInvalidLedgerBalance,
    PreviewInsufficientLedgerBalance,
    PreviewInvalidLimitPricePostOnly,
    PreviewInvalidLimitPrice,
    PreviewInvalidNoLiquidity,
    PreviewInsufficientFund,
    PreviewInvalidCommissionConfiguration,
    PreviewInvalidStopPrice,
    PreviewInvalidBaseSizeTooLarge,
    PreviewInvalidBaseSizeTooSmall,
    PreviewInvalidQuoteSizePrecision,
    PreviewInvalidQuoteSizeTooLarge,
    PreviewInvalidPriceTooLarge,
    PreviewInvalidQuoteSizeTooSmall,
    PreviewInsufficientFundsForFutures,
    PreviewBreachedPriceLimit,
    PreviewBreachedAccountPositionLimit,
    PreviewBreachedCompanyPositionLimit,
    PreviewInvalidMarginHealth,
    PreviewRiskProxyFailure,
    PreviewUntradableFcmAccountStatus,
    PreviewInLiquidation,
    PreviewInvalidMarginType,
    PreviewInvalidLeverage,
    PreviewUntradableProduct,
    PreviewInvalidFcmTradingSession,
    PreviewNotAllowedByMarketState,
    PreviewBreachedOpenInterestLimit,
    PreviewGeofencingRestriction,
    PreviewInvalidEndTime,
    PreviewOppositeMarginTypeExists,
    PreviewQuoteSizeNotAllowedForBracket,
    PreviewInvalidBracketPrices,
    PreviewMissingMarketTradeData,
    PreviewInvalidBracketLimitPrice,
    PreviewInvalidBracketStopTriggerPrice,
    PreviewBracketLimitPriceOutOfBounds,
    PreviewStopTriggerPriceOutOfBounds,
    PreviewBracketOrderNotSupported,
    PreviewInvalidStopPricePrecision,
    PreviewStopPriceAboveLimitPrice,
    PreviewStopPriceBelowLimitPrice,
    PreviewStopPriceAboveLastTradePrice,
    PreviewStopPriceBelowLastTradePrice,
    PreviewFokDisabled,
    PreviewFokOnlyAllowedOnLimitOrders,
    PreviewPostOnlyNotAllowedWithFok,
    PreviewUboHighLeverageQuantityBreached,
    PreviewEcosystemLeverageUtilizationBreached,
    PreviewCloseOnlyFailure,
    PreviewUboHighLeverageNotionalBreached,
    PreviewEndTimeTooFarInFuture,
    PreviewLimitPriceTooFarFromMarket,
    PreviewFuturesAfterHourInvalidOrderType,
    PreviewFuturesAfterHourInvalidTimeInForce,
    PreviewInvalidAttachedTakeProfitPrice,
    PreviewInvalidAttachedStopLossPrice,
    PreviewInvalidAttachedTakeProfitPricePrecision,
    PreviewInvalidAttachedStopLossPricePrecision,
    PreviewInvalidAttachedTakeProfitPriceOutOfBounds,
    PreviewInvalidAttachedStopLossPriceOutOfBounds,
    PreviewInvalidBracketOrderSide,
    PreviewBracketOrderSizeExceedsPosition,
    PreviewOrderSizeExceedsBracketedPosition,
    PreviewInvalidLimitPricePrecision,
    PreviewInvalidStopTriggerPricePrecision,
    PreviewInvalidAttachedTakeProfitPriceExceedsMaxDistanceFromOriginatingPrice,
    PreviewInvalidAttachedTakeProfitSizeBelowMin,
    PreviewAttachedOrderSizeMustBeNil,
    PreviewBelowMinSizeForDuration,
    PreviewMaxDailyVolumeNotionalBreached,
    PreviewInvalidSettlementCurrency,
    PreviewDurationTooSmall,
    PreviewIntxFokOnlyAllowedOnLimitAndMarketOrders,
    PreviewBucketSizeSmallerThanQuoteMin,
    PreviewBucketSizeSmallerThanBaseMin,
    PreviewEndTimeAfterContractExpiration,
    PreviewStartTimeMustBeSpecified,
    PreviewIcebergOrdersNotSupported,
    PreviewEndTimeIsInThePast,
    PreviewGtdOrdersMustHaveEndTime,
    PreviewAttachedOrderMustHavePositivePrices,
    PreviewInvalidOrderSideForAttachedTpsl,
    PreviewAttachedOrdersOnlyAllowedOnMarketLimit,
    PreviewInvalidOrderTypeForAttached,
    PreviewPriceNotAllowedForMarketOrders,
    PreviewReduceOnlyNotAllowedOnVenue,
    PreviewNonNumericOrderSize,
    PreviewInvalidIntxClientOrderId,
    PreviewDurationTooLarge,
    PreviewReduceOnlyNotAllowedOnSpotProducts,
    PreviewLimitOrderPriceExceedsPriceBandOnBuy,
    PreviewLimitOrderPriceExceedsPriceBandOnSell,
    PreviewInvalidAttachedTakeProfitPriceOutOfBoundsOnAggressiveOrder,
    PreviewInvalidAttachedStopLossPriceOutOfBoundsOnAggressiveOrder,
    PreviewStopTriggered,
    PreviewReplaceNotSupported,
    PreviewOrderIsPendingCancel,
    PreviewPositionSizeIncreaseReject,
    PreviewAssetBalanceIncreaseReject,
    PreviewTooManyPendingReplaces,
    PreviewInvalidRfqBaseSizeTooSmall,
    PreviewInvalidRfqBaseSizeTooLarge,
    PreviewInvalidRfqQuoteSizeTooSmall,
    PreviewInvalidRfqQuoteSizeTooLarge,
    PreviewReduceOnlyIncreasedPositionSize,
    PreviewCompliancePurchaseLimitExceeded,
    PreviewScaledParamInfeasible,
    PreviewScaledMinOrderViolation,
    PreviewScaledMaxOrderViolation,
    PreviewPostOnlyNotAllowedWithPeg,
    PreviewInvalidPegOffset,
    PreviewInvalidPegWigLevel,
    PreviewInvalidPegVenueOptions,
    PreviewPegInvalidOrderType,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CreatedOrderNewOrderFailureReason {
    UnknownFailureReason,
    UnsupportedOrderConfiguration,
    InvalidSide,
    InvalidProductId,
    InvalidSizePrecision,
    InvalidPricePrecision,
    InsufficientFund,
    InvalidLedgerBalance,
    OrderEntryDisabled,
    IneligiblePair,
    InvalidLimitPricePostOnly,
    InvalidLimitPrice,
    InvalidNoLiquidity,
    InvalidRequest,
    CommanderRejectedNewOrder,
    InsufficientFunds,
    InLiquidation,
    InvalidMarginType,
    InvalidLeverage,
    UntradableProduct,
    InvalidFcmTradingSession,
    GeofencingRestriction,
    QuoteSizeNotAllowedForBracket,
    InvalidBracketPrices,
    MissingMarketTradeData,
    InvalidBracketLimitPrice,
    InvalidBracketStopTriggerPrice,
    BracketLimitPriceOutOfBounds,
    StopTriggerPriceOutOfBounds,
    BracketOrderNotSupported,
    FokDisabled,
    FokOnlyAllowedOnLimitOrders,
    PostOnlyNotAllowedWithFok,
    UboHighLeverageQuantityBreached,
    EndTimeTooFarInFuture,
    LimitPriceTooFarFromMarket,
    OpenBracketOrders,
    FuturesAfterHourInvalidOrderType,
    FuturesAfterHourInvalidTimeInForce,
    InvalidAttachedTakeProfitPrice,
    InvalidAttachedStopLossPrice,
    InvalidAttachedTakeProfitPricePrecision,
    InvalidAttachedStopLossPricePrecision,
    InvalidAttachedTakeProfitPriceOutOfBounds,
    InvalidAttachedStopLossPriceOutOfBounds,
    InvalidAttachedTakeProfitPriceExceedsMaxDistanceFromOriginatingPrice,
    InvalidAttachedTakeProfitSizeBelowMin,
    AttachedOrderSizeMustBeNil,
    InvalidSettlementCurrency,
    DurationTooSmall,
    IntxFokOnlyAllowedOnLimitAndMarketOrders,
    BucketSizeSmallerThanQuoteMin,
    BucketSizeSmallerThanBaseMin,
    EndTimeAfterContractExpiration,
    StartTimeMustBeSpecified,
    IcebergOrdersNotSupported,
    EndTimeIsInThePast,
    GtdOrdersMustHaveEndTime,
    AttachedOrderMustHavePositivePrices,
    InvalidOrderSideForAttachedTpsl,
    AttachedOrdersOnlyAllowedOnMarketLimit,
    InvalidOrderTypeForAttached,
    PriceNotAllowedForMarketOrders,
    ReduceOnlyNotAllowedOnVenue,
    DurationTooLarge,
    ReduceOnlyNotAllowedOnSpotProducts,
    LimitOrderPriceExceedsPriceBandOnBuy,
    LimitOrderPriceExceedsPriceBandOnSell,
    InvalidAttachedTakeProfitPriceOutOfBoundsOnAggressiveOrder,
    InvalidAttachedStopLossPriceOutOfBoundsOnAggressiveOrder,
    StopAlreadyTriggered,
    ReplaceNotSupported,
    OrderIsPendingCancel,
    PositionSizeIncreaseReject,
    AssetBalanceIncreaseReject,
    TooManyPendingReplaces,
    InvalidRfqBaseSizeTooSmall,
    InvalidRfqBaseSizeTooLarge,
    InvalidRfqQuoteSizeTooSmall,
    InvalidRfqQuoteSizeTooLarge,
    InvalidUnsupportedInstrument,
    ReduceOnlyIncreasedPositionSize,
    ScaledParamInfeasible,
    ScaledMinOrderViolation,
    ScaledMaxOrderViolation,
    PostOnlyNotAllowedWithPeg,
    InvalidPegOffset,
    InvalidPegWigLevel,
    InvalidPegVenueOptions,
    PegInvalidOrderType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderList {
    pub cursor: SmartString<LazyCompact>,
    pub has_next: bool,
    pub orders: Vec<Order>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Order {
    pub completion_percentage: Decimal,
    #[serde(with = "time::serde::iso8601")]
    pub created_time: OffsetDateTime,
    pub filled_size: Decimal,
    pub filled_value: Decimal,
    pub order_configuration: ListOrderConfiguration,
    pub order_id: Uuid,
    pub order_type: OrderType,
    pub product_id: SmartString<LazyCompact>,
    pub side: Side,
    pub size_in_quote: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListOrderConfiguration {
    LimitLimitGtc {
        base_size: Decimal,
        limit_price: Decimal,
        post_only: bool,
        reduce_only: bool,
        rfq_disabled: bool,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderType {
    UnknownOrderType,
    Market,
    Limit,
    Stop,
    StopLimit,
    Bracket,
    Twap,
    RollOpen,
    RollClose,
    Liquidation,
    Scaled,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{advanced::rest::ClientBuilder, test};
    use tracing::info;
    use uuid::Uuid;

    #[tokio::test]
    async fn can_create_and_cancel_order() -> test::Result<()> {
        test::setup()?;

        // Create a client and place an order.
        let client = ClientBuilder::new().build()?;
        let order = client
            .create_order(&CreateOrder::new(
                "KSM-USDC",
                Side::Buy,
                CreateOrderConfiguration::limit_limit_gtc(
                    Decimal::new(25, 1),
                    Decimal::new(65, 1),
                    true,
                ),
            ))
            .await?;
        let order_id = order.order_id().unwrap();

        // Get a list of open orders and assert our order is in there.
        let open_orders = client.list_orders().await?;

        assert!(
            open_orders
                .orders
                .iter()
                .find(|&order| order.order_id == order_id)
                .is_some()
        );

        // Cancel the order and verify success.
        let cancelled = client.cancel_orders(&vec![order_id].into()).await?;

        assert!(
            cancelled
                .results
                .iter()
                .find(|&order| order.success == true && order.order_id == order_id)
                .is_some()
        );

        // Get a list of open orders and assert our order isn't in there.
        let open_orders = client.list_orders().await?;

        assert!(
            open_orders
                .orders
                .iter()
                .find(|&order| order.order_id == order_id)
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn can_cancel_orders() -> test::Result<()> {
        test::setup()?;

        let client = ClientBuilder::new().build()?;
        let cancelled = client
            .cancel_orders(
                &vec![Uuid::try_from("f75e047a-3b5a-4e2d-86cd-b62cc0386c4f").unwrap()].into(),
            )
            .await?;

        info!("cancelled => {}", serde_json::to_string_pretty(&cancelled)?);

        Ok(())
    }

    #[tokio::test]
    async fn can_create_order() -> test::Result<()> {
        test::setup()?;

        let client = ClientBuilder::new().build()?;
        let created = client
            .create_order(&CreateOrder::new(
                "KSM-USDC",
                Side::Buy,
                CreateOrderConfiguration::limit_limit_gtc(
                    Decimal::new(25, 1),
                    Decimal::new(65, 1),
                    true,
                ),
            ))
            .await?;

        info!("created => {}", serde_json::to_string_pretty(&created)?);

        Ok(())
    }

    #[tokio::test]
    async fn can_list_open_orders() -> test::Result<()> {
        test::setup()?;

        let client = ClientBuilder::new().build()?;
        let orders = client.list_orders().await?;

        info!("orders => {}", serde_json::to_string_pretty(&orders)?);

        Ok(())
    }
}
