package cn.itcast.hotel.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class PageResult {
    private Long total;
    private List<HotelDoc> hotels;

    public static PageResult result(Long total,List<HotelDoc> data){
        return new PageResult(total,data);
    }
}
