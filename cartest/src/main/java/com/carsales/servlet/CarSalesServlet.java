package com.carsales.servlet;

import com.carsales.dao.CarSalesDAO;
import com.carsales.model.CarSale;
import com.google.gson.Gson;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.*;
import java.io.*;
import java.util.List;

// 修改URL映射，避免与web.xml中的配置冲突
@WebServlet("/api/car-sales-data/*")
@MultipartConfig(
        fileSizeThreshold = 2097152,  // 2MB
        maxFileSize = 20971520,       // 20MB
        maxRequestSize = 31457280     // 30MB
)
public class CarSalesServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private CarSalesDAO carSalesDAO;
    private Gson gson;

    @Override
    public void init() throws ServletException {
        carSalesDAO = new CarSalesDAO();
        gson = new Gson();
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html; charset=UTF-8");
        response.setCharacterEncoding("UTF-8");

        String pathInfo = request.getPathInfo();
        PrintWriter out = response.getWriter();

        try {
            if (pathInfo == null || pathInfo.equals("/")) {
                // 获取所有汽车销售记录
                List<CarSale> carSales = carSalesDAO.getAllCarSales();
                out.print(gson.toJson(carSales));
            } else {
                // 解析路径中的ID
                String[] pathParts = pathInfo.split("/");
                if (pathParts.length == 2) {
                    try {
                        int id = Integer.parseInt(pathParts[1]);
                        CarSale carSale = carSalesDAO.getCarSaleById(id);
                        if (carSale != null) {
                            out.print(gson.toJson(carSale));
                        } else {
                            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                            out.print(gson.toJson(new Result(false, "汽车销售记录不存在")));
                        }
                    } catch (NumberFormatException e) {
                        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                        out.print(gson.toJson(new Result(false, "无效的ID格式")));
                    }
                } else {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    out.print(gson.toJson(new Result(false, "无效的请求路径")));
                }
            }
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            out.print(gson.toJson(new Result(false, "获取数据失败: " + e.getMessage())));
        } finally {
            out.flush();
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html; charset=UTF-8");
        response.setCharacterEncoding("UTF-8");

        PrintWriter out = response.getWriter();

        try {
            // 从请求体读取CarSale对象
            BufferedReader reader = request.getReader();
            CarSale carSale = gson.fromJson(reader, CarSale.class);

            if (carSale == null) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                out.print(gson.toJson(new Result(false, "无效的请求数据")));
                return;
            }

            // 保存汽车销售记录
            boolean success = carSalesDAO.addCarSale(carSale);
            if (success) {
                response.setStatus(HttpServletResponse.SC_CREATED);
                out.print(gson.toJson(new Result(true, "汽车销售记录添加成功")));
            } else {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                out.print(gson.toJson(new Result(false, "汽车销售记录添加失败")));
            }
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            out.print(gson.toJson(new Result(false, "处理请求失败: " + e.getMessage())));
        } finally {
            out.flush();
        }
    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html; charset=UTF-8");
        response.setCharacterEncoding("UTF-8");

        String pathInfo = request.getPathInfo();
        PrintWriter out = response.getWriter();

        try {
            if (pathInfo == null || pathInfo.split("/").length != 2) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                out.print(gson.toJson(new Result(false, "无效的请求路径")));
                return;
            }

            // 解析路径中的ID
            String[] pathParts = pathInfo.split("/");
            int id = Integer.parseInt(pathParts[1]);

            // 从请求体读取更新后的CarSale对象
            BufferedReader reader = request.getReader();
            CarSale carSale = gson.fromJson(reader, CarSale.class);

            if (carSale == null) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                out.print(gson.toJson(new Result(false, "无效的请求数据")));
                return;
            }

            // 确保ID一致
            carSale.setId(id);

            // 更新汽车销售记录
            boolean success = carSalesDAO.updateCarSale(carSale);
            if (success) {
                out.print(gson.toJson(new Result(true, "汽车销售记录更新成功")));
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                out.print(gson.toJson(new Result(false, "汽车销售记录不存在或更新失败")));
            }
        } catch (NumberFormatException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            out.print(gson.toJson(new Result(false, "无效的ID格式")));
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            out.print(gson.toJson(new Result(false, "处理请求失败: " + e.getMessage())));
        } finally {
            out.flush();
        }
    }

    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html; charset=UTF-8");
        response.setCharacterEncoding("UTF-8");

        String pathInfo = request.getPathInfo();
        PrintWriter out = response.getWriter();

        try {
            if (pathInfo == null || pathInfo.split("/").length != 2) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                out.print(gson.toJson(new Result(false, "无效的请求路径")));
                return;
            }

            // 解析路径中的ID
            String[] pathParts = pathInfo.split("/");
            int id = Integer.parseInt(pathParts[1]);

            // 删除汽车销售记录
            boolean success = carSalesDAO.deleteCarSale(id);
            if (success) {
                out.print(gson.toJson(new Result(true, "汽车销售记录删除成功")));
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                out.print(gson.toJson(new Result(false, "汽车销售记录不存在或删除失败")));
            }
        } catch (NumberFormatException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            out.print(gson.toJson(new Result(false, "无效的ID格式")));
        } catch (Exception e) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            out.print(gson.toJson(new Result(false, "处理请求失败: " + e.getMessage())));
        } finally {
            out.flush();
        }
    }

    // 辅助类用于返回操作结果
    private static class Result {
        private boolean success;
        private String message;

        public Result(boolean success, String message) {
            this.success = success;
            this.message = message;
        }

        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
    }
}