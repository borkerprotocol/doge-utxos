use failure::Error;
use futures::future::*;
use futures::Future;
use futures::Stream;
use hyper::{Body, Client, Request};
use std::collections::HashMap;

pub fn handle_request(
    db: &leveldb_rs::DB,
    rpc: &hyper::http::Uri,
    client: &Client<hyper::client::HttpConnector>,
    uname: Option<&String>,
    pwd: Option<&String>,
    path_and_query: &http::uri::PathAndQuery,
) -> impl Future<Item = UTXORes, Error = Error> {
    match path_and_query.path() {
        "/balance" => Either::A(result((|| {
            let url = url::Url::parse(&format!("http://localhost/{}", path_and_query.as_str()))?;
            let qparams = url
                .query_pairs()
                .collect::<HashMap<std::borrow::Cow<'_, str>, std::borrow::Cow<'_, str>>>();
            let address = qparams
                .get(&std::borrow::Cow::Borrowed("address"))
                .ok_or(format_err!("missing address"))?;
            Ok(UTXORes::Balance(get_balance(db, &address)?))
        })())),
        "/utxos" => Either::B(
            result(
                (|| {
                    let url =
                        url::Url::parse(&format!("http://localhost/{}", path_and_query.as_str()))?;
                    let qparams = url
                        .query_pairs()
                        .collect::<HashMap<std::borrow::Cow<'_, str>, std::borrow::Cow<'_, str>>>();
                    let address = qparams
                        .get(&std::borrow::Cow::Borrowed("address"))
                        .ok_or(format_err!("missing address"))?
                        .to_string();
                    let amount = qparams
                        .get(&std::borrow::Cow::Borrowed("amount"))
                        .ok_or(format_err!("missing amount"))?;
                    let amount = str::parse(&amount)?;
                    let min_count = match qparams.get(&std::borrow::Cow::Borrowed("minCount")) {
                        Some(a) => Some(str::parse(&a)?),
                        None => None,
                    };
                    Ok((address, amount, min_count))
                })()
                .map(|(address, amount, min_count)| {
                    result(get_utxos(
                        db, rpc, client, uname, pwd, &address, amount, min_count,
                    ))
                    .and_then(join_all)
                    .map(|a| UTXORes::UTXOs(a))
                }),
            )
            .and_then(|a| a),
        ),
        _ => Either::A(err(format_err!("unsupported endpoint"))),
    }
}

fn get_balance(db: &leveldb_rs::DB, address: &str) -> Result<u64, Error> {
    let mut address_vec = bitcoin::util::base58::from_check(address)?;
    if address_vec.len() != 21 {
        bail!("invalid address length")
    }
    let mut addr_key = Vec::with_capacity(26);
    addr_key.push(1_u8);
    addr_key.append(&mut address_vec);
    let len = ldb_try!(db.get(&addr_key)).unwrap_or([0_u8; 4].to_vec());
    let mut buf = [0_u8; 4];
    if len.len() == 4 {
        buf.clone_from_slice(&len);
    }
    let len = u32::from_ne_bytes(buf);
    let mut bal = 0_u64;
    addr_key.append(&mut u32::to_ne_bytes(0).to_vec());
    for i in 0..len {
        let i_buf = u32::to_ne_bytes(i);
        addr_key[22..].clone_from_slice(&i_buf);
        let addr_value = ldb_try!(db.get(&addr_key)).ok_or(format_err!("utxo missing"))?;
        let mut val_buf = [0_u8; 8];
        val_buf.clone_from_slice(addr_value.get(36..44).ok_or(format_err!("value missing"))?);
        let val = u64::from_ne_bytes(val_buf);
        bal += val;
    }
    Ok(bal)
}

fn get_utxos(
    db: &leveldb_rs::DB,
    rpc: &hyper::http::Uri,
    client: &Client<hyper::client::HttpConnector>,
    uname: Option<&String>,
    pwd: Option<&String>,
    address: &str,
    amount: u64,
    min_count: Option<usize>,
) -> Result<Vec<impl Future<Item = UTXOData, Error = Error>>, Error> {
    let min_count = min_count.unwrap_or(20);
    let mut address_vec = bitcoin::util::base58::from_check(address)?;
    if address_vec.len() != 21 {
        bail!("invalid address length")
    }
    let mut addr_key = Vec::with_capacity(26);
    addr_key.push(1_u8);
    addr_key.append(&mut address_vec);
    let len = ldb_try!(db.get(&addr_key)).unwrap_or([0_u8; 4].to_vec());
    let mut buf = [0_u8; 4];
    if len.len() == 4 {
        buf.clone_from_slice(&len);
    }
    let len = u32::from_ne_bytes(buf);
    let mut bal = 0_u64;
    let mut utxos = Vec::new();
    addr_key.append(&mut u32::to_ne_bytes(0).to_vec());
    for i in 0..len {
        let i_buf = u32::to_ne_bytes(i);
        addr_key[22..].clone_from_slice(&i_buf);
        let addr_value = ldb_try!(db.get(&addr_key)).ok_or(format_err!("utxo missing"))?;
        let mut txid = [0_u8; 32];
        txid.clone_from_slice(addr_value.get(0..32).ok_or(format_err!("txid missing"))?);
        let mut vout_buf = [0_u8; 4];
        vout_buf.clone_from_slice(addr_value.get(32..36).ok_or(format_err!("vout missing"))?);
        let vout = u32::from_ne_bytes(vout_buf);
        let mut val_buf = [0_u8; 8];
        val_buf.clone_from_slice(addr_value.get(36..44).ok_or(format_err!("value missing"))?);
        let value = u64::from_ne_bytes(val_buf);
        bal += value;
        utxos.push(add_raw(
            client,
            rpc,
            uname,
            pwd,
            UTXODataNoRaw { txid, vout, value },
        )?);
        if i as usize >= min_count && bal >= amount {
            break;
        }
    }
    Ok(utxos)
}

fn add_raw(
    client: &Client<hyper::client::HttpConnector>,
    rpc: &hyper::http::Uri,
    uname: Option<&String>,
    pwd: Option<&String>,
    data: UTXODataNoRaw,
) -> Result<impl Future<Item = UTXOData, Error = Error>, Error> {
    let mut req = Request::builder();
    req.uri(rpc.clone());
    match uname {
        Some(u) => {
            req.header(
                "Authorization",
                base64::encode(&format!("{}:{}", u, pwd.unwrap_or(&"".to_owned()))),
            );
        }
        _ => (),
    };
    let req = req.body(Body::from(serde_json::to_string(&RawTxReq {
        method: "getrawtransaction",
        params: (hex::encode(&data.txid), 0),
    })?))?;
    Ok(client
        .request(req)
        .and_then(|res| res.into_body().concat2())
        .map_err(Error::from)
        .and_then(|res| result(serde_json::from_slice(&res).map_err(Error::from)))
        .and_then(|res: RawTxRes| result(hex::decode(res.result).map_err(Error::from)))
        .map(move |raw| UTXOData {
            txid: data.txid,
            vout: data.vout,
            value: data.value,
            raw,
        }))
}

#[derive(Serialize)]
pub struct RawTxReq {
    method: &'static str,
    params: (String, usize),
}

#[derive(Deserialize)]
pub struct RawTxRes {
    result: String,
}

pub struct UTXODataNoRaw {
    txid: [u8; 32],
    vout: u32,
    value: u64,
}

#[derive(Serialize)]
pub struct UTXOData {
    txid: [u8; 32],
    vout: u32,
    value: u64,
    raw: Vec<u8>,
}
#[derive(Serialize)]
struct UTXODataJSON {
    txid: String,
    vout: u32,
    value: u64,
    raw: String,
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum UTXORes {
    Balance(u64),
    UTXOs(Vec<UTXOData>),
}
impl UTXORes {
    pub fn to_bytes(self) -> Vec<u8> {
        match self {
            UTXORes::Balance(toshis) => u64::to_be_bytes(toshis).to_vec(),
            UTXORes::UTXOs(utxos) => {
                let mut ret = u64::to_be_bytes(utxos.len() as u64).to_vec();
                for mut utxo in utxos {
                    ret.append(&mut utxo.txid.to_vec());
                    ret.append(&mut u32::to_be_bytes(utxo.vout).to_vec());
                    ret.append(&mut u64::to_be_bytes(utxo.value).to_vec());
                    ret.append(&mut utxo.raw)
                }
                ret
            }
        }
    }

    pub fn to_json(self) -> Result<String, Error> {
        match self {
            UTXORes::Balance(toshis) => Ok(serde_json::to_string(&toshis)?),
            UTXORes::UTXOs(utxos) => Ok(serde_json::to_string(
                &utxos
                    .into_iter()
                    .map(|u| UTXODataJSON {
                        txid: hex::encode(u.txid),
                        vout: u.vout,
                        value: u.value,
                        raw: hex::encode(u.raw),
                    })
                    .collect::<Vec<_>>(),
            )?),
        }

    }
}