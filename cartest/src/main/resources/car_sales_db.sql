-- 创建数据库
CREATE DATABASE IF NOT EXISTS car_sales_db;
USE car_sales_db;

-- 创建销售数据表
CREATE TABLE IF NOT EXISTS car_sales (
    id INT PRIMARY KEY AUTO_INCREMENT,
    brand VARCHAR(50) NOT NULL, -- 品牌
    model VARCHAR(100) NOT NULL, -- 车型
    vehicle_type ENUM('passenger', 'commercial') NOT NULL, -- 车辆类型：乘用车/商用车
    region VARCHAR(100) NOT NULL, -- 销售地区
    city VARCHAR(100) NOT NULL, -- 城市
    county VARCHAR(100), -- 区县
    sale_date DATE NOT NULL, -- 销售日期
    sales_volume INT NOT NULL, -- 销售数量
    sales_amount DECIMAL(15, 2) NOT NULL, -- 销售额
    engine_model VARCHAR(50), -- 发动机型号
    fuel_type VARCHAR(20), -- 燃料种类
    displacement DECIMAL(3, 1), -- 排量
    owner_type VARCHAR(50), -- 所有权类型
    user_gender ENUM('male', 'female'), -- 用户性别
    user_age INT -- 用户年龄
);

-- 创建索引以提高查询性能
CREATE INDEX idx_brand ON car_sales(brand);
CREATE INDEX idx_model ON car_sales(model);
CREATE INDEX idx_sale_date ON car_sales(sale_date);
CREATE INDEX idx_region ON car_sales(region);
CREATE INDEX idx_vehicle_type ON car_sales(vehicle_type);