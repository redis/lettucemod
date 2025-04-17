package com.redis.search.query.filter;

public class TestOrListClass {
    public static void main(String[] args) {


        Condition italianCategory = new CategoryCondition("italian");
        Condition priceRange = new PriceCondition(1500, 2000);
        Condition ratingRange = new RatingCondition(4.0, 4.5);
        Condition nameMatch=new NameMatchCondition("olive bristo");
        Condition openmatch= new OpenCondition(true);

        OrList orCondition = new OrList(italianCategory, priceRange, ratingRange,nameMatch, openmatch);

        String redisQuery = orCondition.getQuery();
        System.out.println("Generated Query: " + redisQuery);
    }
}
 class CategoryCondition implements Condition {
    private final String category;

    public CategoryCondition(String category) {
        this.category = category;
    }

    @Override
    public String getQuery() {
        return "@category:{" + category + "}";
    }
}

 class PriceCondition implements Condition {
    private final int min;
    private final int max;

    public PriceCondition(int min, int max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public String getQuery() {
        return "@priceForTwo:[" + min + " " + max + "]";
    }
}

 class RatingCondition implements Condition {
     private final double min;
     private final double max;

     public RatingCondition(double min, double max) {
         this.min = min;
         this.max = max;
     }

     @Override
     public String getQuery() {
         return "@rating:[" + min + " " + max + "]";
     }
 }

     class OpenCondition implements Condition {
         private final boolean isOpen;

         public OpenCondition(boolean isOpen) {
             this.isOpen = isOpen;
         }

         @Override
         public String getQuery() {
             return "@isOpen:{" + isOpen + "}";
         }
     }
      class NameMatchCondition implements Condition {
         private final String name;

         public NameMatchCondition(String name) {
             this.name = name;
         }

         @Override
         public String getQuery() {
             return "@name:~\"" + name + "\"";
         }


 }