package com.ascend.dto;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class StockQuotationInfo implements Serializable {
    private static final long serialVersionUID = 3444922723366143454L;

    private String stockCode;
    private String stockName;
    private long tradeTime;
    private float preClosePrice;
    private float openPrice;
    private float currentPrice;
    private float highPrice;
    private float lowPrice;

    @Override
    public String toString() {
        return this.stockCode + "|" + stockName + "|" + tradeTime + "|" + preClosePrice
                + "|" + openPrice + "|" + currentPrice + "|" + highPrice + "|" + lowPrice;
    }
}
