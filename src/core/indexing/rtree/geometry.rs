use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// A 2D point
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Point { x, y }
    }

    pub fn distance_to(&self, other: &Point) -> f64 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        (dx * dx + dy * dy).sqrt()
    }
}

/// A 2D rectangle defined by min and max coordinates
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Rectangle {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

impl Rectangle {
    pub fn new(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Self {
        Rectangle { min_x, min_y, max_x, max_y }
    }

    pub fn from_points(p1: Point, p2: Point) -> Self {
        Rectangle {
            min_x: p1.x.min(p2.x),
            min_y: p1.y.min(p2.y),
            max_x: p1.x.max(p2.x),
            max_y: p1.y.max(p2.y),
        }
    }

    pub fn from_point(point: Point) -> Self {
        Rectangle {
            min_x: point.x,
            min_y: point.y,
            max_x: point.x,
            max_y: point.y,
        }
    }

    /// Calculate the area of the rectangle
    pub fn area(&self) -> f64 {
        (self.max_x - self.min_x) * (self.max_y - self.min_y)
    }

    /// Calculate the perimeter of the rectangle
    pub fn perimeter(&self) -> f64 {
        2.0 * ((self.max_x - self.min_x) + (self.max_y - self.min_y))
    }

    /// Check if this rectangle contains another rectangle
    pub fn contains(&self, other: &Rectangle) -> bool {
        self.min_x <= other.min_x &&
        self.min_y <= other.min_y &&
        self.max_x >= other.max_x &&
        self.max_y >= other.max_y
    }

    /// Check if this rectangle contains a point
    pub fn contains_point(&self, point: &Point) -> bool {
        self.min_x <= point.x &&
        self.min_y <= point.y &&
        self.max_x >= point.x &&
        self.max_y >= point.y
    }

    /// Check if this rectangle intersects with another rectangle
    pub fn intersects(&self, other: &Rectangle) -> bool {
        !(self.max_x < other.min_x ||
          self.min_x > other.max_x ||
          self.max_y < other.min_y ||
          self.min_y > other.max_y)
    }

    /// Calculate the intersection of two rectangles
    pub fn intersection(&self, other: &Rectangle) -> Option<Rectangle> {
        if !self.intersects(other) {
            return None;
        }

        Some(Rectangle {
            min_x: self.min_x.max(other.min_x),
            min_y: self.min_y.max(other.min_y),
            max_x: self.max_x.min(other.max_x),
            max_y: self.max_y.min(other.max_y),
        })
    }

    /// Calculate the union (bounding box) of two rectangles
    pub fn union(&self, other: &Rectangle) -> Rectangle {
        Rectangle {
            min_x: self.min_x.min(other.min_x),
            min_y: self.min_y.min(other.min_y),
            max_x: self.max_x.max(other.max_x),
            max_y: self.max_y.max(other.max_y),
        }
    }

    /// Calculate the increase in area when unioning with another rectangle
    pub fn area_increase(&self, other: &Rectangle) -> f64 {
        self.union(other).area() - self.area()
    }

    /// Calculate the center point of the rectangle
    pub fn center(&self) -> Point {
        Point::new(
            (self.min_x + self.max_x) / 2.0,
            (self.min_y + self.max_y) / 2.0,
        )
    }

    /// Check if the rectangle is valid (min <= max for both dimensions)
    pub fn is_valid(&self) -> bool {
        self.min_x <= self.max_x && self.min_y <= self.max_y
    }
}

/// Trait for objects that have a bounding box
pub trait BoundingBox {
    fn bounding_box(&self) -> Rectangle;
}

impl BoundingBox for Point {
    fn bounding_box(&self) -> Rectangle {
        Rectangle::from_point(*self)
    }
}

impl BoundingBox for Rectangle {
    fn bounding_box(&self) -> Rectangle {
        *self
    }
}

/// Minimum Bounding Rectangle (MBR) - alias for Rectangle with R-tree specific methods
pub type MBR = Rectangle;

impl MBR {
    /// Calculate the overlap area between two MBRs
    pub fn overlap_area(&self, other: &MBR) -> f64 {
        match self.intersection(other) {
            Some(intersection) => intersection.area(),
            None => 0.0,
        }
    }

    /// Calculate the distance between two MBRs (0 if they intersect)
    pub fn distance_to(&self, other: &MBR) -> f64 {
        if self.intersects(other) {
            return 0.0;
        }

        let dx = if self.max_x < other.min_x {
            other.min_x - self.max_x
        } else if other.max_x < self.min_x {
            self.min_x - other.max_x
        } else {
            0.0
        };

        let dy = if self.max_y < other.min_y {
            other.min_y - self.max_y
        } else if other.max_y < self.min_y {
            self.min_y - other.max_y
        } else {
            0.0
        };

        (dx * dx + dy * dy).sqrt()
    }

    /// Calculate enlargement needed to include another MBR
    pub fn enlargement(&self, other: &MBR) -> f64 {
        self.area_increase(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_creation_and_distance() {
        let p1 = Point::new(0.0, 0.0);
        let p2 = Point::new(3.0, 4.0);
        
        assert_eq!(p1.distance_to(&p2), 5.0); // 3-4-5 triangle
    }

    #[test]
    fn test_rectangle_area_and_perimeter() {
        let rect = Rectangle::new(0.0, 0.0, 10.0, 5.0);
        
        assert_eq!(rect.area(), 50.0);
        assert_eq!(rect.perimeter(), 30.0);
    }

    #[test]
    fn test_rectangle_containment() {
        let big_rect = Rectangle::new(0.0, 0.0, 10.0, 10.0);
        let small_rect = Rectangle::new(2.0, 2.0, 8.0, 8.0);
        let point = Point::new(5.0, 5.0);
        
        assert!(big_rect.contains(&small_rect));
        assert!(!small_rect.contains(&big_rect));
        assert!(big_rect.contains_point(&point));
    }

    #[test]
    fn test_rectangle_intersection() {
        let rect1 = Rectangle::new(0.0, 0.0, 5.0, 5.0);
        let rect2 = Rectangle::new(3.0, 3.0, 8.0, 8.0);
        let rect3 = Rectangle::new(10.0, 10.0, 15.0, 15.0);
        
        assert!(rect1.intersects(&rect2));
        assert!(!rect1.intersects(&rect3));
        
        let intersection = rect1.intersection(&rect2).unwrap();
        assert_eq!(intersection, Rectangle::new(3.0, 3.0, 5.0, 5.0));
        
        assert!(rect1.intersection(&rect3).is_none());
    }

    #[test]
    fn test_rectangle_union() {
        let rect1 = Rectangle::new(0.0, 0.0, 5.0, 5.0);
        let rect2 = Rectangle::new(3.0, 3.0, 8.0, 8.0);
        
        let union = rect1.union(&rect2);
        assert_eq!(union, Rectangle::new(0.0, 0.0, 8.0, 8.0));
    }

    #[test]
    fn test_mbr_distance() {
        let mbr1 = MBR::new(0.0, 0.0, 1.0, 1.0);
        let mbr2 = MBR::new(2.0, 2.0, 3.0, 3.0);
        let mbr3 = MBR::new(0.5, 0.5, 1.5, 1.5);
        
        // Distance between non-intersecting MBRs
        assert!((mbr1.distance_to(&mbr2) - (2.0_f64).sqrt()).abs() < f64::EPSILON);
        
        // Distance between intersecting MBRs should be 0
        assert_eq!(mbr1.distance_to(&mbr3), 0.0);
    }

    #[test]
    fn test_bounding_box_trait() {
        let point = Point::new(5.0, 5.0);
        let rect = Rectangle::new(0.0, 0.0, 10.0, 10.0);
        
        let point_bbox = point.bounding_box();
        assert_eq!(point_bbox, Rectangle::new(5.0, 5.0, 5.0, 5.0));
        
        let rect_bbox = rect.bounding_box();
        assert_eq!(rect_bbox, rect);
    }
} 