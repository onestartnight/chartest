package com.carsales.dao;

import com.carsales.model.CarSale;
import com.carsales.util.DBUtil;
import com.google.gson.Gson;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Date; // 用于数据库操作
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CarSalesDAO {
    private Gson gson = new Gson();

    // 获取所有销售数据（无分页，用于全量查询）
    public List<CarSale> getAllCarSales() {
        List<CarSale> carSales = new ArrayList<>();
        String sql = "SELECT * FROM car_sales ORDER BY id";

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();

            while (rs.next()) {
                CarSale sale = mapResultSetToCarSale(rs);
                carSales.add(sale);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return carSales;
    }

    // 根据ID获取单个销售数据
    public CarSale getCarSaleById(int id) {
        CarSale carSale = null;
        String sql = "SELECT * FROM car_sales WHERE id = ?";

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, id);
            rs = stmt.executeQuery();

            if (rs.next()) {
                carSale = mapResultSetToCarSale(rs);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return carSale;
    }

    // 添加新的销售数据
    public boolean addCarSale(CarSale carSale) {
        String sql = "INSERT INTO car_sales (" +
                "brand, model, vehicle_type, region, city, county, " +
                "sale_date, sales_volume, sales_amount, engine_model, " +
                "fuel_type, displacement, owner_type, user_gender, user_age" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);

            setCarSaleParams(stmt, carSale, false);
            stmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            closeAll(null, stmt, conn);
        }
    }

    // 更新销售数据
    public boolean updateCarSale(CarSale carSale) {
        String sql = "UPDATE car_sales SET " +
                "brand = ?, model = ?, vehicle_type = ?, region = ?, city = ?, county = ?, " +
                "sale_date = ?, sales_volume = ?, sales_amount = ?, engine_model = ?, " +
                "fuel_type = ?, displacement = ?, owner_type = ?, user_gender = ?, user_age = ? " +
                "WHERE id = ?";

        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);

            setCarSaleParams(stmt, carSale, true);
            stmt.setInt(16, carSale.getId()); // 设置WHERE条件的ID
            int rowsUpdated = stmt.executeUpdate();
            return rowsUpdated > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            closeAll(null, stmt, conn);
        }
    }

    // 删除销售数据
    public boolean deleteCarSale(int id) {
        String sql = "DELETE FROM car_sales WHERE id = ?";

        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, id);

            int rowsDeleted = stmt.executeUpdate();
            return rowsDeleted > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            closeAll(null, stmt, conn);
        }
    }

    // 获取所有销售数据，支持分页和筛选（原有方法）
    public List<CarSale> getCarSales(String brand, String model, String region,
                                     String startDate, String endDate, int page, int pageSize) {
        List<CarSale> carSales = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT * FROM car_sales WHERE 1=1");
        List<Object> params = new ArrayList<>();
        if (brand != null && !brand.isEmpty()) {
            sql.append(" AND brand = ?");
            params.add(brand);
        }
        if (model != null && !model.isEmpty()) {
            sql.append(" AND model = ?");
            params.add(model);
        }
        if (region != null && !region.isEmpty()) {
            sql.append(" AND region = ?");
            params.add(region);
        }
        if (startDate != null && !startDate.isEmpty()) {
            sql.append(" AND sale_date >= ?");
            params.add(startDate);
        }
        if (endDate != null && !endDate.isEmpty()) {
            sql.append(" AND sale_date <= ?");
            params.add(endDate);
        }
        sql.append(" LIMIT ? OFFSET ?");

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql.toString());
            int idx = 1;
            for (Object param : params) {
                stmt.setObject(idx++, param);
            }
            stmt.setInt(idx++, pageSize);
            stmt.setInt(idx, (page - 1) * pageSize);

            rs = stmt.executeQuery();
            while (rs.next()) {
                carSales.add(mapResultSetToCarSale(rs));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return carSales;
    }

    // 获取乘用车和商用车的数量和销售额分布（原有方法）
    public Map<String, Object> getVehicleTypeDistribution() {
        Map<String, Object> result = new HashMap<>();
        Map<String, Integer> volumeData = new HashMap<>();
        Map<String, BigDecimal> amountData = new HashMap<>();

        String volumeSql = "SELECT vehicle_type, SUM(sales_volume) as total_volume FROM car_sales GROUP BY vehicle_type";
        String amountSql = "SELECT vehicle_type, SUM(sales_amount) as total_amount FROM car_sales GROUP BY vehicle_type";
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            // 数量分布
            stmt = conn.prepareStatement(volumeSql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                volumeData.put(rs.getString("vehicle_type"), rs.getInt("total_volume"));
            }
            rs.close(); stmt.close();

            // 销售额分布
            stmt = conn.prepareStatement(amountSql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                amountData.put(rs.getString("vehicle_type"), rs.getBigDecimal("total_amount"));
            }
            rs.close(); stmt.close();

            result.put("volume", volumeData);
            result.put("amount", amountData);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return result;
    }

    // 获取某年每个月的汽车销售数量（原有方法）
    public Map<String, Integer> getMonthlySalesByYear(int year) {
        Map<String, Integer> result = new LinkedHashMap<>();
        String sql = "SELECT MONTH(sale_date) as month, SUM(sales_volume) as total_volume " +
                "FROM car_sales WHERE YEAR(sale_date) = ? GROUP BY MONTH(sale_date) ORDER BY month";
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, year);
            rs = stmt.executeQuery();
            while (rs.next()) {
                int month = rs.getInt("month");
                result.put(month + "月", rs.getInt("total_volume"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return result;
    }

    // 获取某个月份各区县的汽车销售数量（原有方法）
    public Map<String, Integer> getCountySalesByMonth(String yearMonth) {
        Map<String, Integer> result = new LinkedHashMap<>();
        String sql = "SELECT county, SUM(sales_volume) as total_volume FROM car_sales WHERE DATE_FORMAT(sale_date, '%Y-%m') = ? GROUP BY county ORDER BY total_volume DESC";
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, yearMonth);
            rs = stmt.executeQuery();
            while (rs.next()) {
                result.put(rs.getString("county"), rs.getInt("total_volume"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return result;
    }

    // 获取购车用户的男女比例（原有方法）
    public Map<String, Integer> getUserGenderRatio() {
        Map<String, Integer> result = new HashMap<>();
        String sql = "SELECT user_gender, SUM(sales_volume) as total_volume FROM car_sales WHERE user_gender IS NOT NULL GROUP BY user_gender";
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                result.put(rs.getString("user_gender"), rs.getInt("total_volume"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return result;
    }

    // 获取不同所有权、型号和类型汽车的销售数量（原有方法）
    public Map<String, Map<String, Integer>> getSalesByOwnerTypeAndModel() {
        Map<String, Map<String, Integer>> result = new HashMap<>();
        String sql = "SELECT owner_type, vehicle_type, model, SUM(sales_volume) as total_volume FROM car_sales GROUP BY owner_type, vehicle_type, model";
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String ownerType = rs.getString("owner_type");
                String vehicleType = rs.getString("vehicle_type");
                String model = rs.getString("model");
                int volume = rs.getInt("total_volume");
                String key = vehicleType + "-" + model;
                result.computeIfAbsent(ownerType, k -> new HashMap<>()).put(key, volume);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return result;
    }

    // 获取不同车型的用户年龄和性别分布（原有方法）
    public Map<String, Map<String, Map<String, Integer>>> getAgeGenderByModel() {
        Map<String, Map<String, Map<String, Integer>>> result = new HashMap<>();
        String sql = "SELECT model, user_gender, " +
                "CASE WHEN user_age < 20 THEN '10-19' " +
                "WHEN user_age < 30 THEN '20-29' " +
                "WHEN user_age < 40 THEN '30-39' " +
                "WHEN user_age < 50 THEN '40-49' " +
                "WHEN user_age < 60 THEN '50-59' " +
                "ELSE '60+' END as age_group, SUM(sales_volume) as total_volume " +
                "FROM car_sales WHERE user_gender IS NOT NULL AND user_age IS NOT NULL GROUP BY model, user_gender, age_group";
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String model = rs.getString("model");
                String gender = rs.getString("user_gender");
                String ageGroup = rs.getString("age_group");
                int volume = rs.getInt("total_volume");
                result.computeIfAbsent(model, k -> new HashMap<>())
                        .computeIfAbsent(gender, k -> new HashMap<>())
                        .put(ageGroup, volume);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return result;
    }

    // 获取不同车型销售数据（原有方法）
    public Map<String, Map<String, Object>> getModelSalesData() {
        Map<String, Map<String, Object>> result = new HashMap<>();
        String sql = "SELECT model, brand, SUM(sales_volume) as total_volume, SUM(sales_amount) as total_amount FROM car_sales GROUP BY model, brand";
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String model = rs.getString("model");
                String brand = rs.getString("brand");
                int volume = rs.getInt("total_volume");
                BigDecimal amount = rs.getBigDecimal("total_amount");
                Map<String, Object> data = new HashMap<>();
                data.put("brand", brand);
                data.put("volume", volume);
                data.put("amount", amount);
                result.put(model, data);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return result;
    }

    // 按照品牌统计发动机型号和燃料种类（原有方法）
    public Map<String, Map<String, Map<String, Integer>>> getEngineFuelByBrand() {
        Map<String, Map<String, Map<String, Integer>>> result = new HashMap<>();
        String sql = "SELECT brand, engine_model, fuel_type, SUM(sales_volume) as total_volume FROM car_sales WHERE engine_model IS NOT NULL AND fuel_type IS NOT NULL GROUP BY brand, engine_model, fuel_type";
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String brand = rs.getString("brand");
                String engine = rs.getString("engine_model");
                String fuel = rs.getString("fuel_type");
                int volume = rs.getInt("total_volume");
                result.computeIfAbsent(brand, k -> new HashMap<>())
                        .computeIfAbsent(engine, k -> new HashMap<>())
                        .put(fuel, volume);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return result;
    }

    // 同排量不同品牌汽车的销售量（原有方法）
    public Map<Double, Map<String, Integer>> getSalesByDisplacementAndBrand() {
        Map<Double, Map<String, Integer>> result = new LinkedHashMap<>();
        String sql = "SELECT displacement, brand, SUM(sales_volume) as total_volume FROM car_sales WHERE displacement IS NOT NULL GROUP BY displacement, brand ORDER BY displacement";
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                double displacement = rs.getDouble("displacement");
                String brand = rs.getString("brand");
                int volume = rs.getInt("total_volume");
                result.computeIfAbsent(displacement, k -> new HashMap<>()).put(brand, volume);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return result;
    }

    // 获取品牌列表（原有方法）
    public List<String> getBrands() {
        List<String> brands = new ArrayList<>();
        String sql = "SELECT DISTINCT brand FROM car_sales ORDER BY brand";
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                brands.add(rs.getString("brand"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return brands;
    }

    // 获取指定品牌的车型列表（原有方法）
    public List<String> getModelsByBrand(String brand) {
        List<String> models = new ArrayList<>();
        String sql = "SELECT DISTINCT model FROM car_sales WHERE brand = ? ORDER BY model";
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, brand);
            rs = stmt.executeQuery();
            while (rs.next()) {
                models.add(rs.getString("model"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return models;
    }

    // 获取地区列表（原有方法）
    public List<String> getRegions() {
        List<String> regions = new ArrayList<>();
        String sql = "SELECT DISTINCT region FROM car_sales ORDER BY region";
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            while (rs.next()) {
                regions.add(rs.getString("region"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return regions;
    }

    // 获取某品牌或某车型的销量随时间变化趋势（原有方法）
    public Map<String, Integer> getSalesTrend(String brand, String model, String startDate, String endDate) {
        Map<String, Integer> result = new LinkedHashMap<>();
        StringBuilder sql = new StringBuilder("SELECT DATE_FORMAT(sale_date, '%Y-%m-%d') as sale_day, SUM(sales_volume) as total_volume FROM car_sales WHERE 1=1");
        List<Object> params = new ArrayList<>();
        if (brand != null && !brand.isEmpty()) {
            sql.append(" AND brand = ?");
            params.add(brand);
        }
        if (model != null && !model.isEmpty()) {
            sql.append(" AND model = ?");
            params.add(model);
        }
        if (startDate != null && !startDate.isEmpty()) {
            sql.append(" AND sale_date >= ?");
            params.add(startDate);
        }
        if (endDate != null && !endDate.isEmpty()) {
            sql.append(" AND sale_date <= ?");
            params.add(endDate);
        }
        sql.append(" GROUP BY sale_day ORDER BY sale_day");
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql.toString());
            int idx = 1;
            for (Object param : params) {
                stmt.setObject(idx++, param);
            }
            rs = stmt.executeQuery();
            while (rs.next()) {
                result.put(rs.getString("sale_day"), rs.getInt("total_volume"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return result;
    }

    // 获取不同车型或不同品牌的销量对比（原有方法）
    public Map<String, Integer> getSalesComparison(String type, String startDate, String endDate) {
        Map<String, Integer> result = new LinkedHashMap<>();
        String groupBy = "brand".equals(type) ? "brand" : "model";
        StringBuilder sql = new StringBuilder("SELECT " + groupBy + ", SUM(sales_volume) as total_volume FROM car_sales WHERE 1=1");
        List<Object> params = new ArrayList<>();
        if (startDate != null && !startDate.isEmpty()) {
            sql.append(" AND sale_date >= ?");
            params.add(startDate);
        }
        if (endDate != null && !endDate.isEmpty()) {
            sql.append(" AND sale_date <= ?");
            params.add(endDate);
        }
        sql.append(" GROUP BY " + groupBy + " ORDER BY total_volume DESC");

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql.toString());
            int idx = 1;
            for (Object param : params) {
                stmt.setObject(idx++, param);
            }
            rs = stmt.executeQuery();
            while (rs.next()) {
                result.put(rs.getString(groupBy), rs.getInt("total_volume"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return result;
    }

    // 获取不同车型或地区的销售占比（原有方法）
    public Map<String, Double> getSalesProportion(String type, String startDate, String endDate) {
        Map<String, Double> result = new HashMap<>();
        String groupBy = "region".equals(type) ? "region" : "model";

        StringBuilder totalSql = new StringBuilder("SELECT SUM(sales_volume) as total FROM car_sales WHERE 1=1");
        List<Object> params = new ArrayList<>();
        if (startDate != null && !startDate.isEmpty()) {
            totalSql.append(" AND sale_date >= ?");
            params.add(startDate);
        }
        if (endDate != null && !endDate.isEmpty()) {
            totalSql.append(" AND sale_date <= ?");
            params.add(endDate);
        }

        StringBuilder groupSql = new StringBuilder("SELECT " + groupBy + ", SUM(sales_volume) as volume FROM car_sales WHERE 1=1");
        if (startDate != null && !startDate.isEmpty()) {
            groupSql.append(" AND sale_date >= ?");
        }
        if (endDate != null && !endDate.isEmpty()) {
            groupSql.append(" AND sale_date <= ?");
        }
        groupSql.append(" GROUP BY " + groupBy);

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            // 查询总销量
            stmt = conn.prepareStatement(totalSql.toString());
            int idx = 1;
            for (Object param : params) {
                stmt.setObject(idx++, param);
            }
            rs = stmt.executeQuery();
            int total = 0;
            if (rs.next()) {
                total = rs.getInt("total");
            }
            rs.close(); stmt.close();

            if (total == 0) return result;

            // 查询分组销量
            stmt = conn.prepareStatement(groupSql.toString());
            idx = 1;
            for (Object param : params) {
                stmt.setObject(idx++, param);
            }
            rs = stmt.executeQuery();
            while (rs.next()) {
                String name = rs.getString(groupBy);
                int volume = rs.getInt("volume");
                result.put(name, (double) volume / total * 100);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return result;
    }

    // 获取当月总销量（原有方法）
    public int getCurrentMonthSales() {
        String sql = "SELECT SUM(sales_volume) as total_volume FROM car_sales WHERE DATE_FORMAT(sale_date, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')";
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int total = 0;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            if (rs.next()) {
                total = rs.getInt("total_volume");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return total;
    }

    // 获取各车型销量排行（原有方法）
    public Map<String, Integer> getModelSalesRanking(int limit) {
        Map<String, Integer> result = new LinkedHashMap<>();
        String sql = "SELECT model, SUM(sales_volume) as total_volume FROM car_sales GROUP BY model ORDER BY total_volume DESC LIMIT ?";
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            stmt = conn.prepareStatement(sql);
            stmt.setInt(1, limit);
            rs = stmt.executeQuery();
            while (rs.next()) {
                result.put(rs.getString("model"), rs.getInt("total_volume"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(rs, stmt, conn);
        }
        return result;
    }

    // CSV文件导入功能（原有方法）
    public boolean importFromCSV(String csvContent) {
        String sql = "INSERT INTO car_sales (" +
                "brand, model, vehicle_type, region, city, county, " +
                "sale_date, sales_volume, sales_amount, engine_model, " +
                "fuel_type, displacement, owner_type, user_gender, user_age" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DBUtil.getConnection();
            conn.setAutoCommit(false);
            stmt = conn.prepareStatement(sql);

            String[] lines = csvContent.split("\n");
            for (int i = 1; i < lines.length; i++) { // 跳过表头
                String line = lines[i].trim();
                if (line.isEmpty()) continue;
                String[] fields = line.split(",", -1);
                if (fields.length < 15) continue; // 字段数量不够

                try {
                    stmt.setString(1, fields[0].trim());
                    stmt.setString(2, fields[1].trim());
                    stmt.setString(3, fields[2].trim());
                    stmt.setString(4, fields[3].trim());
                    stmt.setString(5, fields[4].trim());
                    stmt.setString(6, fields[5].trim());
                    // sale_date 用 java.sql.Date 解析
                    stmt.setDate(7, Date.valueOf(fields[6].trim()));
                    stmt.setInt(8, Integer.parseInt(fields[7].trim()));
                    stmt.setBigDecimal(9, new BigDecimal(fields[8].trim()));
                    stmt.setString(10, fields[9].trim());
                    stmt.setString(11, fields[10].trim());
                    stmt.setDouble(12, Double.parseDouble(fields[11].trim()));
                    stmt.setString(13, fields[12].trim());
                    stmt.setString(14, fields[13].trim());
                    stmt.setInt(15, Integer.parseInt(fields[14].trim()));
                    stmt.addBatch();
                } catch (Exception e) {
                    // 某一条数据格式有误，跳过即可
                    continue;
                }
                if ((i % 500 == 0) || (i == lines.length - 1)) {
                    stmt.executeBatch();
                }
            }
            stmt.executeBatch();
            conn.commit();
            return true;
        } catch (Exception e) {
            if (conn != null) {
                try { conn.rollback(); } catch (SQLException ex) { ex.printStackTrace(); }
            }
            e.printStackTrace();
            return false;
        } finally {
            closeAll(null, stmt, conn);
        }
    }

    // 统一资源关闭（原有方法）
    private void closeAll(ResultSet rs, Statement stmt, Connection conn) {
        try { if (rs != null) rs.close(); } catch (SQLException ignored) {}
        try { if (stmt != null) stmt.close(); } catch (SQLException ignored) {}
        try { if (conn != null) conn.close(); } catch (SQLException ignored) {}
    }

    // 工具方法：将ResultSet映射为CarSale对象
    private CarSale mapResultSetToCarSale(ResultSet rs) throws SQLException {
        CarSale sale = new CarSale();
        sale.setId(rs.getInt("id"));
        sale.setBrand(rs.getString("brand"));
        sale.setModel(rs.getString("model"));
        sale.setVehicleType(rs.getString("vehicle_type"));
        sale.setRegion(rs.getString("region"));
        sale.setCity(rs.getString("city"));
        sale.setCounty(rs.getString("county"));
        // sale_date 用 java.sql.Date 转成 java.util.Date
        Date dbDate = rs.getDate("sale_date");
        if (dbDate != null) {
            sale.setSaleDate(new java.util.Date(dbDate.getTime()));
        } else {
            sale.setSaleDate(null);
        }
        sale.setSalesVolume(rs.getInt("sales_volume"));
        sale.setSalesAmount(rs.getBigDecimal("sales_amount"));
        sale.setEngineModel(rs.getString("engine_model"));
        sale.setFuelType(rs.getString("fuel_type"));
        sale.setDisplacement(rs.getDouble("displacement"));
        sale.setOwnerType(rs.getString("owner_type"));
        sale.setUserGender(rs.getString("user_gender"));
        sale.setUserAge(rs.getInt("user_age"));
        return sale;
    }

    // 工具方法：设置CarSale参数到PreparedStatement
    private void setCarSaleParams(PreparedStatement stmt, CarSale carSale, boolean isUpdate) throws SQLException {
        stmt.setString(1, carSale.getBrand());
        stmt.setString(2, carSale.getModel());
        stmt.setString(3, carSale.getVehicleType());
        stmt.setString(4, carSale.getRegion());
        stmt.setString(5, carSale.getCity());
        stmt.setString(6, carSale.getCounty());

        // 处理日期转换
        if (carSale.getSaleDate() != null) {
            stmt.setDate(7, new Date(carSale.getSaleDate().getTime()));
        } else {
            stmt.setNull(7, java.sql.Types.DATE);
        }

        stmt.setInt(8, carSale.getSalesVolume());
        stmt.setBigDecimal(9, carSale.getSalesAmount());
        stmt.setString(10, carSale.getEngineModel());
        stmt.setString(11, carSale.getFuelType());
        stmt.setDouble(12, carSale.getDisplacement());
        stmt.setString(13, carSale.getOwnerType());
        stmt.setString(14, carSale.getUserGender());

        // 处理年龄可能为null的情况
        stmt.setInt(15, carSale.getUserAge());
    }
}