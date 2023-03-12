package cn.itcast.hotel.pojo;

import lombok.Data;

import java.io.Serializable;

@Data
public class Param implements Serializable {
    private String key;
    private Integer page;
    private Integer size;
    private String sortBy;
    private String city;
    private String startName;
    private String brand;
    private Integer minPrice;
    private Integer maxPrice;
}
