// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0
//! Sorted-set commands. Mirrors Python's sorted-set command surface.
#![allow(clippy::type_complexity)]

use crate::commands::options::{ConditionalChange, Limit, UpdateOptions};
use crate::error::Result;
use crate::executor::CommandExecutor;
use crate::value;
use async_trait::async_trait;
use bytes::Bytes;
use redis::{Cmd, ToRedisArgs};

/// A score boundary for `ZRANGEBYSCORE`/`ZCOUNT` etc.
///
/// Mirrors Python `ScoreBoundary` + `InfBound`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScoreBound {
    /// Negative infinity (`-inf`).
    NegativeInfinity,
    /// Positive infinity (`+inf`).
    PositiveInfinity,
    /// Inclusive bound at `value`.
    Inclusive(f64),
    /// Exclusive bound at `value` (`(value`).
    Exclusive(f64),
}

impl ScoreBound {
    fn to_arg(self) -> String {
        match self {
            ScoreBound::NegativeInfinity => "-inf".to_string(),
            ScoreBound::PositiveInfinity => "+inf".to_string(),
            ScoreBound::Inclusive(v) => v.to_string(),
            ScoreBound::Exclusive(v) => format!("({v}"),
        }
    }
}

/// A lexicographical boundary for `ZRANGEBYLEX`/`ZLEXCOUNT`.
///
/// Mirrors Python `LexBoundary` + `InfBound`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexBound {
    /// Smallest possible value (`-`).
    NegativeInfinity,
    /// Largest possible value (`+`).
    PositiveInfinity,
    /// Inclusive bound (`[value`).
    Inclusive(Vec<u8>),
    /// Exclusive bound (`(value`).
    Exclusive(Vec<u8>),
}

impl LexBound {
    fn to_arg(&self) -> Vec<u8> {
        match self {
            LexBound::NegativeInfinity => b"-".to_vec(),
            LexBound::PositiveInfinity => b"+".to_vec(),
            LexBound::Inclusive(v) => {
                let mut out = vec![b'['];
                out.extend_from_slice(v);
                out
            }
            LexBound::Exclusive(v) => {
                let mut out = vec![b'('];
                out.extend_from_slice(v);
                out
            }
        }
    }
}

/// Aggregation mode for `ZUNIONSTORE`/`ZINTERSTORE`.
///
/// Mirrors Python `AggregationType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationType {
    /// Sum the scores.
    Sum,
    /// Take the minimum score.
    Min,
    /// Take the maximum score.
    Max,
}

impl AggregationType {
    fn as_arg(&self) -> &'static str {
        match self {
            AggregationType::Sum => "SUM",
            AggregationType::Min => "MIN",
            AggregationType::Max => "MAX",
        }
    }
}

/// Which end of a sorted set `ZMPOP`/`BZMPOP` should pop from.
///
/// Mirrors Python `ScoreFilter`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScoreFilter {
    /// Pop the members with the lowest scores (`MIN`).
    Min,
    /// Pop the members with the highest scores (`MAX`).
    Max,
}

impl ScoreFilter {
    pub(crate) fn as_arg(&self) -> &'static str {
        match self {
            ScoreFilter::Min => "MIN",
            ScoreFilter::Max => "MAX",
        }
    }
}

/// Options for the `ZADD` command.
///
/// Mirrors the option surface of Python's `zadd(...)`: a conditional change
/// (`NX`/`XX`), an update condition (`GT`/`LT`), and the `CH` flag which makes
/// `ZADD` return the number of changed (added + updated) elements rather than
/// just the number added.
#[derive(Debug, Clone, Copy, Default)]
pub struct ZAddOptions {
    /// Conditional set (`NX`/`XX`).
    pub conditional_change: Option<ConditionalChange>,
    /// Update condition (`GT`/`LT`).
    pub update_condition: Option<UpdateOptions>,
    /// Return the count of changed elements (`CH`).
    pub changed: bool,
}

impl ZAddOptions {
    pub(crate) fn add_to(&self, cmd: &mut Cmd) {
        if let Some(c) = &self.conditional_change {
            c.add_to(cmd);
        }
        if let Some(u) = &self.update_condition {
            cmd.arg(u.as_arg());
        }
        if self.changed {
            cmd.arg("CH");
        }
    }
}

/// Sorted-set commands (`ZADD`, `ZRANGE`, `ZSCORE`, ...).
#[async_trait]
pub trait SortedSetCommands: CommandExecutor {
    /// Add members with scores (`ZADD`); returns the number of new members.
    async fn zadd<K: ToRedisArgs + Send, M: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        members_scores: &[(M, f64)],
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("ZADD").arg(key);
        for (m, s) in members_scores {
            cmd.arg(s).arg(m);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Increment the score of `member` by `increment` (`ZADD ... INCR`).
    async fn zadd_incr<K: ToRedisArgs + Send, M: ToRedisArgs + Send>(
        &self,
        key: K,
        member: M,
        increment: f64,
    ) -> Result<Option<f64>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZADD")
            .arg(key)
            .arg("INCR")
            .arg(increment)
            .arg(member);
        value::to_opt_f64(self.execute_command(cmd, None).await?)
    }

    /// Remove members (`ZREM`); returns members removed.
    async fn zrem<K: ToRedisArgs + Send, M: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        members: &[M],
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("ZREM").arg(key);
        for m in members {
            cmd.arg(m);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get the score of `member` (`ZSCORE`).
    async fn zscore<K: ToRedisArgs + Send, M: ToRedisArgs + Send>(
        &self,
        key: K,
        member: M,
    ) -> Result<Option<f64>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZSCORE").arg(key).arg(member);
        value::to_opt_f64(self.execute_command(cmd, None).await?)
    }

    /// Get scores of multiple members (`ZMSCORE`).
    async fn zmscore<K: ToRedisArgs + Send, M: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        members: &[M],
    ) -> Result<Vec<Option<f64>>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZMSCORE").arg(key);
        for m in members {
            cmd.arg(m);
        }
        match self.execute_command(cmd, None).await? {
            redis::Value::Array(items) => items.into_iter().map(value::to_opt_f64).collect(),
            other => Ok(vec![value::to_opt_f64(other)?]),
        }
    }

    /// Get the number of members in the sorted set (`ZCARD`).
    async fn zcard<K: ToRedisArgs + Send>(&self, key: K) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("ZCARD").arg(key);
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Count members with scores within `[min, max]` (`ZCOUNT`).
    async fn zcount<K: ToRedisArgs + Send>(
        &self,
        key: K,
        min: ScoreBound,
        max: ScoreBound,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("ZCOUNT")
            .arg(key)
            .arg(min.to_arg())
            .arg(max.to_arg());
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Count members in a lexicographical range (`ZLEXCOUNT`).
    async fn zlexcount<K: ToRedisArgs + Send>(
        &self,
        key: K,
        min: &LexBound,
        max: &LexBound,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("ZLEXCOUNT")
            .arg(key)
            .arg(min.to_arg())
            .arg(max.to_arg());
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get a range of members by index (`ZRANGE key start stop`).
    async fn zrange_by_index<K: ToRedisArgs + Send>(
        &self,
        key: K,
        start: i64,
        stop: i64,
        rev: bool,
    ) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZRANGE").arg(key).arg(start).arg(stop);
        if rev {
            cmd.arg("REV");
        }
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    /// Get a range of members by index with scores (`ZRANGE ... WITHSCORES`).
    async fn zrange_withscores<K: ToRedisArgs + Send>(
        &self,
        key: K,
        start: i64,
        stop: i64,
        rev: bool,
    ) -> Result<Vec<(Bytes, f64)>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZRANGE").arg(key).arg(start).arg(stop);
        if rev {
            cmd.arg("REV");
        }
        cmd.arg("WITHSCORES");
        collect_member_scores(self.execute_command(cmd, None).await?)
    }

    /// Get members with scores within a range (`ZRANGEBYSCORE`).
    async fn zrangebyscore<K: ToRedisArgs + Send>(
        &self,
        key: K,
        min: ScoreBound,
        max: ScoreBound,
    ) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZRANGEBYSCORE")
            .arg(key)
            .arg(min.to_arg())
            .arg(max.to_arg());
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    /// Get the rank (index) of `member`, low to high (`ZRANK`).
    async fn zrank<K: ToRedisArgs + Send, M: ToRedisArgs + Send>(
        &self,
        key: K,
        member: M,
    ) -> Result<Option<i64>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZRANK").arg(key).arg(member);
        match self.execute_command(cmd, None).await? {
            redis::Value::Nil => Ok(None),
            other => Ok(Some(value::to_i64(other)?)),
        }
    }

    /// Get the rank of `member`, high to low (`ZREVRANK`).
    async fn zrevrank<K: ToRedisArgs + Send, M: ToRedisArgs + Send>(
        &self,
        key: K,
        member: M,
    ) -> Result<Option<i64>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZREVRANK").arg(key).arg(member);
        match self.execute_command(cmd, None).await? {
            redis::Value::Nil => Ok(None),
            other => Ok(Some(value::to_i64(other)?)),
        }
    }

    /// Increment the score of `member` by `increment` (`ZINCRBY`).
    async fn zincrby<K: ToRedisArgs + Send, M: ToRedisArgs + Send>(
        &self,
        key: K,
        increment: f64,
        member: M,
    ) -> Result<f64> {
        let mut cmd = Cmd::new();
        cmd.arg("ZINCRBY").arg(key).arg(increment).arg(member);
        value::to_f64(self.execute_command(cmd, None).await?)
    }

    /// Pop the member with the lowest score (`ZPOPMIN`).
    async fn zpopmin<K: ToRedisArgs + Send>(&self, key: K) -> Result<Vec<(Bytes, f64)>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZPOPMIN").arg(key);
        collect_member_scores(self.execute_command(cmd, None).await?)
    }

    /// Pop the member with the highest score (`ZPOPMAX`).
    async fn zpopmax<K: ToRedisArgs + Send>(&self, key: K) -> Result<Vec<(Bytes, f64)>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZPOPMAX").arg(key);
        collect_member_scores(self.execute_command(cmd, None).await?)
    }

    /// Get a random member from the sorted set (`ZRANDMEMBER`).
    async fn zrandmember<K: ToRedisArgs + Send>(&self, key: K) -> Result<Option<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZRANDMEMBER").arg(key);
        value::to_opt_bytes(self.execute_command(cmd, None).await?)
    }

    /// Store the intersection of sorted sets into `destination` (`ZINTERSTORE`).
    async fn zinterstore<D: ToRedisArgs + Send, K: ToRedisArgs + Send + Sync>(
        &self,
        destination: D,
        keys: &[K],
        aggregate: Option<AggregationType>,
    ) -> Result<i64> {
        self.zsetop_store("ZINTERSTORE", destination, keys, aggregate)
            .await
    }

    /// Store the union of sorted sets into `destination` (`ZUNIONSTORE`).
    async fn zunionstore<D: ToRedisArgs + Send, K: ToRedisArgs + Send + Sync>(
        &self,
        destination: D,
        keys: &[K],
        aggregate: Option<AggregationType>,
    ) -> Result<i64> {
        self.zsetop_store("ZUNIONSTORE", destination, keys, aggregate)
            .await
    }

    /// Add members with scores and options (`ZADD` with `NX`/`XX`/`GT`/`LT`/`CH`);
    /// returns the number of added (or, with `CH`, changed) members.
    async fn zadd_options<K: ToRedisArgs + Send, M: ToRedisArgs + Send + Sync>(
        &self,
        key: K,
        members_scores: &[(M, f64)],
        options: ZAddOptions,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("ZADD").arg(key);
        options.add_to(&mut cmd);
        for (m, s) in members_scores {
            cmd.arg(s).arg(m);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get `count` random members (`ZRANDMEMBER key count`). A negative count
    /// allows repeated members.
    async fn zrandmember_count<K: ToRedisArgs + Send>(
        &self,
        key: K,
        count: i64,
    ) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZRANDMEMBER").arg(key).arg(count);
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    /// Get `count` random members with scores (`ZRANDMEMBER key count WITHSCORES`).
    async fn zrandmember_withscores<K: ToRedisArgs + Send>(
        &self,
        key: K,
        count: i64,
    ) -> Result<Vec<(Bytes, f64)>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZRANDMEMBER").arg(key).arg(count).arg("WITHSCORES");
        collect_member_scores(self.execute_command(cmd, None).await?)
    }

    /// Incrementally iterate a sorted set (`ZSCAN`). Returns `(cursor, [(member,
    /// score), ...])`. A returned cursor of `"0"` indicates completion.
    async fn zscan<K: ToRedisArgs + Send>(
        &self,
        key: K,
        cursor: &str,
        pattern: Option<&[u8]>,
        count: Option<i64>,
    ) -> Result<(String, Vec<(Bytes, f64)>)> {
        let mut cmd = Cmd::new();
        cmd.arg("ZSCAN").arg(key).arg(cursor);
        if let Some(p) = pattern {
            cmd.arg("MATCH").arg(p);
        }
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        let (cursor, flat) =
            crate::commands::generic::parse_scan_reply(self.execute_command(cmd, None).await?)?;
        // ZSCAN returns a flat [member, score, member, score, ...] list.
        let mut out = Vec::with_capacity(flat.len() / 2);
        let mut iter = flat.into_iter();
        while let (Some(m), Some(s)) = (iter.next(), iter.next()) {
            let score: f64 = String::from_utf8_lossy(&s)
                .parse()
                .map_err(|_| crate::error::GlideError::Request("invalid ZSCAN score".into()))?;
            out.push((m, score));
        }
        Ok((cursor, out))
    }

    /// Blocking pop of the member with the lowest score across `keys`
    /// (`BZPOPMIN`). Returns `(key, member, score)` or `None` on timeout.
    async fn bzpopmin<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
        timeout: f64,
    ) -> Result<Option<(Bytes, Bytes, f64)>> {
        self.bzpop("BZPOPMIN", keys, timeout).await
    }

    /// Blocking pop of the member with the highest score across `keys`
    /// (`BZPOPMAX`). Returns `(key, member, score)` or `None` on timeout.
    async fn bzpopmax<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
        timeout: f64,
    ) -> Result<Option<(Bytes, Bytes, f64)>> {
        self.bzpop("BZPOPMAX", keys, timeout).await
    }

    #[doc(hidden)]
    async fn bzpop<K: ToRedisArgs + Send + Sync>(
        &self,
        op: &'static str,
        keys: &[K],
        timeout: f64,
    ) -> Result<Option<(Bytes, Bytes, f64)>> {
        let mut cmd = Cmd::new();
        cmd.arg(op);
        for k in keys {
            cmd.arg(k);
        }
        cmd.arg(timeout);
        match self.execute_command(cmd, None).await? {
            redis::Value::Nil => Ok(None),
            redis::Value::Array(mut items) if items.len() == 3 => {
                let score = value::to_f64(items.pop().unwrap())?;
                let member = value::to_bytes(items.pop().unwrap())?;
                let key = value::to_bytes(items.pop().unwrap())?;
                Ok(Some((key, member, score)))
            }
            other => Err(crate::error::GlideError::Request(format!(
                "unexpected blocking zpop reply: {other:?}"
            ))),
        }
    }

    /// Pop members from the first non-empty sorted set (`ZMPOP`). Returns
    /// `(key, [(member, score), ...])` or `None` if all sets are empty.
    async fn zmpop<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
        filter: ScoreFilter,
        count: Option<i64>,
    ) -> Result<Option<(Bytes, Vec<(Bytes, f64)>)>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZMPOP").arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        cmd.arg(filter.as_arg());
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        parse_zmpop(self.execute_command(cmd, None).await?)
    }

    /// Blocking variant of `ZMPOP` (`BZMPOP`). Returns `(key, [(member, score),
    /// ...])` or `None` on timeout.
    async fn bzmpop<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
        filter: ScoreFilter,
        timeout: f64,
        count: Option<i64>,
    ) -> Result<Option<(Bytes, Vec<(Bytes, f64)>)>> {
        let mut cmd = Cmd::new();
        cmd.arg("BZMPOP").arg(timeout).arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        cmd.arg(filter.as_arg());
        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }
        parse_zmpop(self.execute_command(cmd, None).await?)
    }

    /// Store a range of a sorted set into `destination` (`ZRANGESTORE` by index).
    /// Returns the number of elements stored.
    async fn zrangestore_by_index<D: ToRedisArgs + Send, S: ToRedisArgs + Send>(
        &self,
        destination: D,
        source: S,
        start: i64,
        stop: i64,
        rev: bool,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("ZRANGESTORE")
            .arg(destination)
            .arg(source)
            .arg(start)
            .arg(stop);
        if rev {
            cmd.arg("REV");
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Store into `destination` the members of the sorted set `source` whose
    /// scores are within `[min, max]` (`ZRANGESTORE ... BYSCORE`), returning the
    /// number of elements stored.
    ///
    /// When `rev` is `true` the range is interpreted in reverse (highest scores
    /// first); the bounds are emitted in the order the server requires for `REV`.
    /// `limit` applies an optional `LIMIT offset count`.
    async fn zrangestore_by_score<D: ToRedisArgs + Send, S: ToRedisArgs + Send>(
        &self,
        destination: D,
        source: S,
        min: ScoreBound,
        max: ScoreBound,
        rev: bool,
        limit: Option<Limit>,
    ) -> Result<i64> {
        // For a reverse range the server expects the high bound first.
        let (first, second) = if rev { (max, min) } else { (min, max) };
        let mut cmd = Cmd::new();
        cmd.arg("ZRANGESTORE")
            .arg(destination)
            .arg(source)
            .arg(first.to_arg())
            .arg(second.to_arg())
            .arg("BYSCORE");
        if rev {
            cmd.arg("REV");
        }
        if let Some(limit) = limit {
            cmd.arg("LIMIT").arg(limit.offset).arg(limit.count);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Store into `destination` the members of the sorted set `source` whose
    /// lexicographical values are within `[min, max]`
    /// (`ZRANGESTORE ... BYLEX`), returning the number of elements stored.
    ///
    /// When `rev` is `true` the range is interpreted in reverse; the bounds are
    /// emitted in the order the server requires for `REV`. `limit` applies an
    /// optional `LIMIT offset count`.
    async fn zrangestore_by_lex<D: ToRedisArgs + Send, S: ToRedisArgs + Send>(
        &self,
        destination: D,
        source: S,
        min: &LexBound,
        max: &LexBound,
        rev: bool,
        limit: Option<Limit>,
    ) -> Result<i64> {
        let (first, second) = if rev { (max, min) } else { (min, max) };
        let mut cmd = Cmd::new();
        cmd.arg("ZRANGESTORE")
            .arg(destination)
            .arg(source)
            .arg(first.to_arg())
            .arg(second.to_arg())
            .arg("BYLEX");
        if rev {
            cmd.arg("REV");
        }
        if let Some(limit) = limit {
            cmd.arg("LIMIT").arg(limit.offset).arg(limit.count);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Compute the difference of the given sorted sets (`ZDIFF`).
    async fn zdiff<K: ToRedisArgs + Send + Sync>(&self, keys: &[K]) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZDIFF").arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    /// Compute the difference of the given sorted sets with scores
    /// (`ZDIFF ... WITHSCORES`).
    async fn zdiff_withscores<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
    ) -> Result<Vec<(Bytes, f64)>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZDIFF").arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        cmd.arg("WITHSCORES");
        collect_member_scores(self.execute_command(cmd, None).await?)
    }

    /// Store the difference of the given sorted sets into `destination`
    /// (`ZDIFFSTORE`).
    async fn zdiffstore<D: ToRedisArgs + Send, K: ToRedisArgs + Send + Sync>(
        &self,
        destination: D,
        keys: &[K],
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("ZDIFFSTORE").arg(destination).arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Compute the union of the given sorted sets (`ZUNION`).
    async fn zunion<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
        aggregate: Option<AggregationType>,
    ) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZUNION").arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        if let Some(agg) = aggregate {
            cmd.arg("AGGREGATE").arg(agg.as_arg());
        }
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    /// Compute the union of the given sorted sets with scores
    /// (`ZUNION ... WITHSCORES`).
    async fn zunion_withscores<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
        aggregate: Option<AggregationType>,
    ) -> Result<Vec<(Bytes, f64)>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZUNION").arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        if let Some(agg) = aggregate {
            cmd.arg("AGGREGATE").arg(agg.as_arg());
        }
        cmd.arg("WITHSCORES");
        collect_member_scores(self.execute_command(cmd, None).await?)
    }

    /// Compute the intersection of the given sorted sets (`ZINTER`).
    async fn zinter<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
        aggregate: Option<AggregationType>,
    ) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZINTER").arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        if let Some(agg) = aggregate {
            cmd.arg("AGGREGATE").arg(agg.as_arg());
        }
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    /// Compute the intersection of the given sorted sets with scores
    /// (`ZINTER ... WITHSCORES`).
    async fn zinter_withscores<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
        aggregate: Option<AggregationType>,
    ) -> Result<Vec<(Bytes, f64)>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZINTER").arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        if let Some(agg) = aggregate {
            cmd.arg("AGGREGATE").arg(agg.as_arg());
        }
        cmd.arg("WITHSCORES");
        collect_member_scores(self.execute_command(cmd, None).await?)
    }

    /// Cardinality of the intersection of the given sorted sets (`ZINTERCARD`),
    /// with an optional `LIMIT`.
    async fn zintercard<K: ToRedisArgs + Send + Sync>(
        &self,
        keys: &[K],
        limit: Option<i64>,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("ZINTERCARD").arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        if let Some(l) = limit {
            cmd.arg("LIMIT").arg(l);
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get the rank of `member` with its score, low to high (`ZRANK ... WITHSCORE`).
    async fn zrank_withscore<K: ToRedisArgs + Send, M: ToRedisArgs + Send>(
        &self,
        key: K,
        member: M,
    ) -> Result<Option<(i64, f64)>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZRANK").arg(key).arg(member).arg("WITHSCORE");
        parse_rank_withscore(self.execute_command(cmd, None).await?)
    }

    /// Get the rank of `member` with its score, high to low
    /// (`ZREVRANK ... WITHSCORE`).
    async fn zrevrank_withscore<K: ToRedisArgs + Send, M: ToRedisArgs + Send>(
        &self,
        key: K,
        member: M,
    ) -> Result<Option<(i64, f64)>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZREVRANK").arg(key).arg(member).arg("WITHSCORE");
        parse_rank_withscore(self.execute_command(cmd, None).await?)
    }

    /// Remove members ranked within `[start, stop]` (`ZREMRANGEBYRANK`).
    async fn zremrangebyrank<K: ToRedisArgs + Send>(
        &self,
        key: K,
        start: i64,
        stop: i64,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("ZREMRANGEBYRANK").arg(key).arg(start).arg(stop);
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Remove members scored within `[min, max]` (`ZREMRANGEBYSCORE`).
    async fn zremrangebyscore<K: ToRedisArgs + Send>(
        &self,
        key: K,
        min: ScoreBound,
        max: ScoreBound,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("ZREMRANGEBYSCORE")
            .arg(key)
            .arg(min.to_arg())
            .arg(max.to_arg());
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Remove members in a lexicographical range (`ZREMRANGEBYLEX`).
    async fn zremrangebylex<K: ToRedisArgs + Send>(
        &self,
        key: K,
        min: &LexBound,
        max: &LexBound,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg("ZREMRANGEBYLEX")
            .arg(key)
            .arg(min.to_arg())
            .arg(max.to_arg());
        value::to_i64(self.execute_command(cmd, None).await?)
    }

    /// Get members in a lexicographical range (`ZRANGEBYLEX`).
    async fn zrangebylex<K: ToRedisArgs + Send>(
        &self,
        key: K,
        min: &LexBound,
        max: &LexBound,
    ) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZRANGEBYLEX")
            .arg(key)
            .arg(min.to_arg())
            .arg(max.to_arg());
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    /// Get members scored within a range, high to low (`ZREVRANGEBYSCORE`).
    async fn zrevrangebyscore<K: ToRedisArgs + Send>(
        &self,
        key: K,
        max: ScoreBound,
        min: ScoreBound,
    ) -> Result<Vec<Bytes>> {
        let mut cmd = Cmd::new();
        cmd.arg("ZREVRANGEBYSCORE")
            .arg(key)
            .arg(max.to_arg())
            .arg(min.to_arg());
        collect_bytes(self.execute_command(cmd, None).await?)
    }

    #[doc(hidden)]
    async fn zsetop_store<D: ToRedisArgs + Send, K: ToRedisArgs + Send + Sync>(
        &self,
        op: &'static str,
        destination: D,
        keys: &[K],
        aggregate: Option<AggregationType>,
    ) -> Result<i64> {
        let mut cmd = Cmd::new();
        cmd.arg(op).arg(destination).arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        if let Some(agg) = aggregate {
            cmd.arg("AGGREGATE").arg(agg.as_arg());
        }
        value::to_i64(self.execute_command(cmd, None).await?)
    }
}

/// Parse a `ZRANK ... WITHSCORE` reply (`[rank, score]` or nil).
fn parse_rank_withscore(v: redis::Value) -> Result<Option<(i64, f64)>> {
    match v {
        redis::Value::Nil => Ok(None),
        redis::Value::Array(mut items) if items.len() == 2 => {
            let score = value::to_f64(items.pop().unwrap())?;
            let rank = value::to_i64(items.pop().unwrap())?;
            Ok(Some((rank, score)))
        }
        other => Err(crate::error::GlideError::Request(format!(
            "unexpected ZRANK WITHSCORE reply: {other:?}"
        ))),
    }
}

/// Parse a `ZMPOP`/`BZMPOP` reply `[key, [[member, score], ...]]` into
/// `Option<(key, [(member, score), ...])>`.
fn parse_zmpop(v: redis::Value) -> Result<Option<(Bytes, Vec<(Bytes, f64)>)>> {
    match v {
        redis::Value::Nil => Ok(None),
        redis::Value::Array(mut items) if items.len() == 2 => {
            let members = collect_member_scores(items.pop().unwrap())?;
            let key = value::to_bytes(items.pop().unwrap())?;
            Ok(Some((key, members)))
        }
        other => Err(crate::error::GlideError::Request(format!(
            "unexpected ZMPOP reply: {other:?}"
        ))),
    }
}

fn collect_bytes(v: redis::Value) -> Result<Vec<Bytes>> {
    match v {
        redis::Value::Array(items) => items.into_iter().map(value::to_bytes).collect(),
        redis::Value::Nil => Ok(Vec::new()),
        other => Ok(vec![value::to_bytes(other)?]),
    }
}

/// Parse a `WITHSCORES`/`ZPOPMIN`-style reply into `(member, score)` pairs,
/// handling both RESP2 flat arrays and RESP3 nested pairs.
fn collect_member_scores(v: redis::Value) -> Result<Vec<(Bytes, f64)>> {
    match v {
        redis::Value::Nil => Ok(Vec::new()),
        // RESP3 returns a map of member -> score.
        redis::Value::Map(pairs) => pairs
            .into_iter()
            .map(|(m, s)| Ok((value::to_bytes(m)?, value::to_f64(s)?)))
            .collect(),
        redis::Value::Array(items) => {
            // RESP3: array of [member, score] pairs.
            if items
                .iter()
                .all(|it| matches!(it, redis::Value::Array(inner) if inner.len() == 2))
            {
                let mut out = Vec::with_capacity(items.len());
                for it in items {
                    if let redis::Value::Array(mut pair) = it {
                        let score = value::to_f64(pair.pop().unwrap())?;
                        let member = value::to_bytes(pair.pop().unwrap())?;
                        out.push((member, score));
                    }
                }
                Ok(out)
            } else {
                // RESP2: flat [member, score, member, score, ...].
                let mut out = Vec::with_capacity(items.len() / 2);
                let mut iter = items.into_iter();
                while let (Some(m), Some(s)) = (iter.next(), iter.next()) {
                    out.push((value::to_bytes(m)?, value::to_f64(s)?));
                }
                Ok(out)
            }
        }
        other => Err(crate::error::GlideError::Request(format!(
            "unexpected sorted-set reply: {other:?}"
        ))),
    }
}

impl<T: CommandExecutor + ?Sized> SortedSetCommands for T {}

#[cfg(test)]
mod tests {
    use super::*;

    fn args_of(cmd: &Cmd) -> Vec<String> {
        cmd.args_iter()
            .filter_map(|a| match a {
                redis::Arg::Simple(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
                redis::Arg::Cursor => None,
            })
            .collect()
    }

    #[test]
    fn score_filter_args() {
        assert_eq!(ScoreFilter::Min.as_arg(), "MIN");
        assert_eq!(ScoreFilter::Max.as_arg(), "MAX");
    }

    #[test]
    fn zadd_options_full_ordering() {
        let opts = ZAddOptions {
            conditional_change: Some(ConditionalChange::OnlyIfExists),
            update_condition: Some(UpdateOptions::GreaterThan),
            changed: true,
        };
        let mut cmd = Cmd::new();
        opts.add_to(&mut cmd);
        assert_eq!(args_of(&cmd), vec!["XX", "GT", "CH"]);
    }

    #[test]
    fn zadd_options_empty() {
        let mut cmd = Cmd::new();
        ZAddOptions::default().add_to(&mut cmd);
        assert!(args_of(&cmd).is_empty());
    }

    #[test]
    fn score_bound_formatting() {
        assert_eq!(ScoreBound::NegativeInfinity.to_arg(), "-inf");
        assert_eq!(ScoreBound::PositiveInfinity.to_arg(), "+inf");
        assert_eq!(ScoreBound::Exclusive(1.0).to_arg(), "(1");
    }
}
