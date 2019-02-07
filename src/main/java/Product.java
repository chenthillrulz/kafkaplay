import com.fasterxml.jackson.annotation.JsonProperty;

public class Product {
    @JsonProperty("sam_product_id")
    public String samProductId;
    @JsonProperty("sam_product_name")
    public String samProductName;
    @JsonProperty("pp2_product_id")
    public String pp2ProductId;
    @JsonProperty("error")
    public String error;
}
