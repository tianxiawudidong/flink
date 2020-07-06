package com.ifchange.flink.mysql;

import java.util.*;

/**
 * 数据集封装
 *
 * @author Administrator
 *
 */
public class DataTable {

    public String[] column;//列字段
    public String[][] row; //行值
    public int rowCount = 0;//行数
    public int colCount = 0;//列数

    public DataTable() {
        super();
    }

    public DataTable(String[] column, String[][] row, int rowCount, int colCount) {
        super();
        this.column = column;
        this.row = row;
        this.rowCount = rowCount;
        this.colCount = colCount;
    }

    public void setDataTable(List<HashMap<String, String>> list) {
        rowCount = list.size();
        colCount = list.get(0).size();
        column = new String[colCount];
        row = new String[rowCount][colCount];
        for (int i = 0; i < rowCount; i++) {
            Set<Map.Entry<String, String>> set = list.get(i).entrySet();
            int j = 0;
            for (Iterator<Map.Entry<String, String>> it = set.iterator(); it
                .hasNext();) {
                Map.Entry<String, String> entry = (Map.Entry<String, String>) it
                    .next();
                row[i][j] = entry.getValue();
                if (i == rowCount - 1) {
                    column[j] = entry.getKey();
                }
                j++;
            }
        }
    }

    public String[] getColumn() {
        return column;
    }

    public void setColumn(String[] column) {
        this.column = column;
    }

    public String[][] getRow() {
        return row;
    }

    public void setRow(String[][] row) {
        this.row = row;
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public int getColCount() {
        return colCount;
    }

    public void setColCount(int colCount) {
        this.colCount = colCount;
    }
}
