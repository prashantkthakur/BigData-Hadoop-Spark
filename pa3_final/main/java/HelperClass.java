import java.io.Serializable;
import java.util.ArrayList;

public class HelperClass{

    public static class LinkClass implements Serializable{
        // from: to
        private String from;
        private ArrayList<String> to=new ArrayList <>();

        public String getFrom(){return from;}
        public ArrayList<String> getTo(){return to;}
        public void setFrom(String from){this.from = from;}
        public void setTo(Iterable<String> tos){for (String n: tos){this.to.add(n);}}
    }

    public static class PageRank implements Serializable {
        private String id;
        private Double rank;

        public Double getRank() {
            return rank;
        }

        public void setRank(Double rank) {
            this.rank = rank;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }


    }

    public static  class TitleClass implements Serializable {
        private Long id;
        private String title;

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Long getId() {
            return id;
        }

    }

}
