use nom::{
    bytes::complete::{tag, take_until},
    combinator::opt,
    number::complete::float,
    sequence::{delimited, preceded, terminated, tuple},
    IResult,
};
use rocket::routes;
use sqlx::SqlitePool;
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

fn read_p1() {
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
        rocket::tokio::task::spawn(async move {
            let conn = SqlitePool::connect_lazy("sqlite://./dev.db").unwrap();
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
                .execute(&conn)
                .await
                .expect("insert into db");

            println!("{:#?}", frame);
        });
    }
}

#[rocket::get("/")]
fn index() -> &'static str {
    "hi"
}

#[rocket::launch]
async fn rocket() -> _ {
    rocket::tokio::task::spawn_blocking(read_p1);

    rocket::build().mount("/", routes![index])
}
