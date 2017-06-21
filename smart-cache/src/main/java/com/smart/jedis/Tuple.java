package com.smart.jedis;

/**
 * Tuple
 * -----------------------------------------------------------------------------------------------------------------------------------
 * 
 * @author YRain
 */
public class Tuple {

    private Object element;
    private Double score;

    public Tuple(Object element, Double score) {
        super();
        this.element = element;
        this.score = score;
    }

    public Object getElement() {
        return element;
    }

    public void setElement(Object element) {
        this.element = element;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

}
