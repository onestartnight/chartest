package com.carsales.model;

import java.math.BigDecimal;
import java.util.Date;

public class CarSale {
    private int id;
    private String brand;
    private String model;
    private String vehicleType;
    private String region;
    private String city;
    private String county;
    private Date saleDate;
    private int salesVolume;
    private BigDecimal salesAmount;
    private String engineModel;
    private String fuelType;
    private double displacement;
    private String ownerType;
    private String userGender;
    private int userAge;

    // 构造函数、getter和setter方法
    public CarSale() {}

    public int getId() { return id; }
    public void setId(int id) { this.id = id; }
    public String getBrand() { return brand; }
    public void setBrand(String brand) { this.brand = brand; }
    public String getModel() { return model; }
    public void setModel(String model) { this.model = model; }
    public String getVehicleType() { return vehicleType; }
    public void setVehicleType(String vehicleType) { this.vehicleType = vehicleType; }
    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }
    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }
    public String getCounty() { return county; }
    public void setCounty(String county) { this.county = county; }
    public Date getSaleDate() { return saleDate; }
    public void setSaleDate(Date saleDate) { this.saleDate = saleDate; }
    public int getSalesVolume() { return salesVolume; }
    public void setSalesVolume(int salesVolume) { this.salesVolume = salesVolume; }
    public BigDecimal getSalesAmount() { return salesAmount; }
    public void setSalesAmount(BigDecimal salesAmount) { this.salesAmount = salesAmount; }
    public String getEngineModel() { return engineModel; }
    public void setEngineModel(String engineModel) { this.engineModel = engineModel; }
    public String getFuelType() { return fuelType; }
    public void setFuelType(String fuelType) { this.fuelType = fuelType; }
    public double getDisplacement() { return displacement; }
    public void setDisplacement(double displacement) { this.displacement = displacement; }
    public String getOwnerType() { return ownerType; }
    public void setOwnerType(String ownerType) { this.ownerType = ownerType; }
    public String getUserGender() { return userGender; }
    public void setUserGender(String userGender) { this.userGender = userGender; }
    public int getUserAge() { return userAge; }
    public void setUserAge(int userAge) { this.userAge = userAge; }
}