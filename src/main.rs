use nom::{
    bytes::complete::{tag, take_until},
    combinator::opt,
    number::complete::float,
    sequence::{delimited, preceded, terminated, tuple},
    IResult,
};
use rocket::{State, http::Status, routes, serde::json::Json};
use serde::Serialize;
use sqlx::{FromRow, SqlitePool};
use std::{
    io::{BufRead, BufReader},
    mem,
    time::Duration,
};

#[derive(Debug, Default)]
struct DataFrame {
    delivered_1: Option<f32>,
    delivered_2: Option<f32>,
    received_1: Option<f32>,
    received_2: Option<f32>,
    current_tariff: Option<i32>,
    actual_delivered: Option<f32>,
    actual_received: Option<f32>,
    max_power: Option<f32>,
    switch_mode: Option<i32>,
}

fn parse_float(id: &'static str) -> impl Fn(&str) -> IResult<&str, f32> {
    move |s| {
        preceded(
            tag(id),
            delimited(
                tag("("),
                terminated(
                    float,
                    opt(tuple((
                        tag("*"),
                        take_until(")"),
                    ))),
                ),
                tag(")"),
            ),
        )(s)
    }
}

fn try_field_f32(line: &str, id: &'static str, field: &mut Option<f32>) {
    if let Ok((_, res)) = parse_float(id)(line) {
        field.replace(res);
    }
}

fn try_field_i32(line: &str, id: &'static str, field: &mut Option<i32>) {
    if let Ok((_, res)) = parse_float(id)(line) {
        field.replace(res as i32);
    }
}

fn read_p1(db_pool: SqlitePool) {
    let serial = serialport::new("/dev/ttyUSB0", 9600)
        .timeout(Duration::from_secs(15))
        .data_bits(serialport::DataBits::Seven)
        .parity(serialport::Parity::Even)
        .open()
        .expect("opening serial device");

    let p1 = BufReader::new(serial);

    let mut frame = DataFrame::default();
    let frames = p1
        .lines()
        .filter_map(Result::ok)
        .filter_map(|line| {
            if line == "!" {
                return Some(mem::take(&mut frame));
            }
            else {
                try_field_f32(&line, "1-0:1.8.1", &mut frame.delivered_1);
                try_field_f32(&line, "1-0:1.8.2", &mut frame.delivered_2);
                try_field_f32(&line, "1-0:2.8.1", &mut frame.received_1);
                try_field_f32(&line, "1-0:2.8.2", &mut frame.received_2);
                try_field_i32(&line, "0-0:96.14.0", &mut frame.current_tariff);
                try_field_f32(&line, "1-0:1.7.0", &mut frame.actual_delivered);
                try_field_f32(&line, "1-0:2.7.0", &mut frame.actual_received);
                try_field_f32(&line, "0-0:17.0.0", &mut frame.max_power);
                try_field_i32(&line, "0-0:96.3.10", &mut frame.switch_mode);
            }

            None
        });

    for frame in frames {
        let db_pool = db_pool.clone();
        rocket::tokio::task::spawn(async move {
            let now = chrono::Utc::now().timestamp();

            sqlx::query!(
                "INSERT INTO records VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                now,
                frame.delivered_1,
                frame.delivered_2,
                frame.received_1,
                frame.received_2,
                frame.current_tariff,
                frame.actual_delivered,
                frame.actual_received,
                frame.max_power,
                frame.switch_mode,
            )
                .execute(&db_pool)
                .await
                .expect("insert into db");

            println!("{:#?}", frame);
        });
    }
}

#[derive(FromRow)]
struct Record {
    timestamp: i64,
    delivered_1: Option<f32>,
    delivered_2: Option<f32>,
}

impl Record {
    pub fn watts_since(&self, other: &Record) -> f32 {
        let mut sorted = [self, other];
        sorted.sort_by_key(|r| r.timestamp);
        let [ min, max ] = sorted;

        (max.delivered_2.unwrap() - min.delivered_2.unwrap()) * 3600.0 / (max.timestamp - min.timestamp) as f32 * 1000.0
    }
}

#[rocket::get("/loadavg")]
async fn loadavg(db_pool: &State<SqlitePool>) -> Result<String, (Status, String)> {
    let now = chrono::Utc::now().timestamp();

    let res = sqlx::query_as!(
            Record,
            "SELECT timestamp, delivered_1, delivered_2 FROM records WHERE timestamp > ? - 15 * 60 - 10",
            now
        )
        .fetch_all(db_pool.inner())
        .await
        .map_err(|e| (Status::InternalServerError, e.to_string()))?;

    if res.is_empty() {
        return Err((Status::InternalServerError, "no records".to_owned()));
    }

    let min_15_start = res.iter().min_by_key(|r| r.timestamp).unwrap();
    let min_5_start = res.iter().filter(|r| r.timestamp > now - 5 * 60 - 10).min_by_key(|r| r.timestamp).unwrap();
    let min_1_start = res.iter().filter(|r| r.timestamp > now - 1 * 60 - 10).min_by_key(|r| r.timestamp).unwrap();
    let end = res.last().unwrap();

    Ok(format!(
        "{:.0}, {:.0}, {:.0}",
        end.watts_since(min_1_start),
        end.watts_since(min_5_start),
        end.watts_since(min_15_start),
    ))
}

#[derive(Serialize)]
struct ValuesResponse {
    timestamp: i64,
    delivered_1: Option<f32>,
    delivered_2: Option<f32>,
    current_tariff: Option<i64>,
}

#[rocket::get("/values?<from>&<to>")]
async fn values(
    from: Option<i64>,
    to: Option<i64>,
    db_pool: &State<SqlitePool>,
) -> Result<Json<Vec<ValuesResponse>>, (Status, String)> {
    let (from, to) = if let (Some(from), Some(to)) = (from, to) { (from, to) }
    else {
        return Err((Status::BadRequest, "`from` and `to` query parameters are required".to_owned()));
    };

    let res = sqlx::query_as!(
            ValuesResponse,
            "SELECT timestamp, delivered_1, delivered_2, current_tariff FROM records WHERE timestamp BETWEEN ? AND ?",
            from,
            to,
        )
        .fetch_all(db_pool.inner())
        .await
        .map_err(|e| (Status::InternalServerError, e.to_string()))?;

    Ok(Json(res))
}

#[rocket::launch]
async fn rocket() -> _ {
    let db_pool = SqlitePool::connect_lazy("sqlite://./dev.db").unwrap();

    rocket::tokio::task::spawn_blocking({
        let db_pool = db_pool.clone();
        move || read_p1(db_pool)
    });

    rocket::build()
        .manage(db_pool)
        .mount("/", routes![ values, loadavg ])
}
