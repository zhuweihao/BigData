package com.zhuweihao.POJO;

/**
 * @Author zhuweihao
 * @Date 2022/12/7 16:28
 * @Description com.zhuweihao.POJO
 */
public class lineorder {
    private int lo_orderkey;
    private int lo_linenumber;
    private int lo_custkey;
    private int lo_partkey;
    private int lo_suppkey;
    private int lo_orderdate;
    private String lo_orderpriority;
    private int lo_shippriority;
    private int lo_quantity;
    private int lo_extendedprice;
    private int lo_ordtotalprice;
    private int lo_discount;
    private int lo_revenue;
    private int lo_supplycost;
    private int lo_tax;
    private int lo_commitdate;
    private String lo_shipmode;

    public lineorder() {
    }

    @Override
    public String toString() {
        return "lineorder{" +
                "lo_orderkey=" + lo_orderkey +
                ", lo_linenumber=" + lo_linenumber +
                ", lo_custkey=" + lo_custkey +
                ", lo_suppkey=" + lo_suppkey +
                ", lo_orderdate=" + lo_orderdate +
                ", lo_orderpriority='" + lo_orderpriority + '\'' +
                ", lo_shippriority=" + lo_shippriority +
                ", lo_quantity=" + lo_quantity +
                ", lo_extendedprice=" + lo_extendedprice +
                ", lo_ordtotalprice=" + lo_ordtotalprice +
                ", lo_discount=" + lo_discount +
                ", lo_revenue=" + lo_revenue +
                ", lo_supplycost=" + lo_supplycost +
                ", lo_tax=" + lo_tax +
                ", lo_commitdate=" + lo_commitdate +
                ", lo_shipmode='" + lo_shipmode + '\'' +
                '}';
    }

    public int getLo_orderkey() {
        return lo_orderkey;
    }

    public void setLo_orderkey(int lo_orderkey) {
        this.lo_orderkey = lo_orderkey;
    }

    public int getLo_linenumber() {
        return lo_linenumber;
    }

    public void setLo_linenumber(int lo_linenumber) {
        this.lo_linenumber = lo_linenumber;
    }

    public int getLo_custkey() {
        return lo_custkey;
    }

    public void setLo_custkey(int lo_custkey) {
        this.lo_custkey = lo_custkey;
    }

    public int getLo_partkey() {
        return lo_partkey;
    }

    public void setLo_partkey(int lo_partkey) {
        this.lo_partkey = lo_partkey;
    }

    public int getLo_suppkey() {
        return lo_suppkey;
    }

    public void setLo_suppkey(int lo_suppkey) {
        this.lo_suppkey = lo_suppkey;
    }

    public int getLo_orderdate() {
        return lo_orderdate;
    }

    public void setLo_orderdate(int lo_orderdate) {
        this.lo_orderdate = lo_orderdate;
    }

    public String getLo_orderpriority() {
        return lo_orderpriority;
    }

    public void setLo_orderpriority(String lo_orderpriority) {
        this.lo_orderpriority = lo_orderpriority;
    }

    public int getLo_shippriority() {
        return lo_shippriority;
    }

    public void setLo_shippriority(int lo_shippriority) {
        this.lo_shippriority = lo_shippriority;
    }

    public int getLo_quantity() {
        return lo_quantity;
    }

    public void setLo_quantity(int lo_quantity) {
        this.lo_quantity = lo_quantity;
    }

    public int getLo_extendedprice() {
        return lo_extendedprice;
    }

    public void setLo_extendedprice(int lo_extendedprice) {
        this.lo_extendedprice = lo_extendedprice;
    }

    public int getLo_ordtotalprice() {
        return lo_ordtotalprice;
    }

    public void setLo_ordtotalprice(int lo_ordtotalprice) {
        this.lo_ordtotalprice = lo_ordtotalprice;
    }

    public int getLo_discount() {
        return lo_discount;
    }

    public void setLo_discount(int lo_discount) {
        this.lo_discount = lo_discount;
    }

    public int getLo_revenue() {
        return lo_revenue;
    }

    public void setLo_revenue(int lo_revenue) {
        this.lo_revenue = lo_revenue;
    }

    public int getLo_supplycost() {
        return lo_supplycost;
    }

    public void setLo_supplycost(int lo_supplycost) {
        this.lo_supplycost = lo_supplycost;
    }

    public int getLo_tax() {
        return lo_tax;
    }

    public void setLo_tax(int lo_tax) {
        this.lo_tax = lo_tax;
    }

    public int getLo_commitdate() {
        return lo_commitdate;
    }

    public void setLo_commitdate(int lo_commitdate) {
        this.lo_commitdate = lo_commitdate;
    }

    public String getLo_shipmode() {
        return lo_shipmode;
    }

    public void setLo_shipmode(String lo_shipmode) {
        this.lo_shipmode = lo_shipmode;
    }
}
