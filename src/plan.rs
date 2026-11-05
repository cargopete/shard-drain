//! Deciding what to do with each deployment in the shard being drained.
//! Pure, so the rules can be tested without a database.

use crate::catalog::{Assignment, Detail, Site};

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Copy into the target shard, switch queries to the copy, and give the
    /// copy the source's assignment.
    Move { node: String },
    /// Already unused by graph-node's own rules; reclaiming it is all that
    /// is left.
    Reclaim,
    /// Needs a decision from the operator first.
    Blocked(String),
}

pub struct Input<'a> {
    pub site: &'a Site,
    pub detail: Option<&'a Detail>,
    pub current_or_pending: bool,
    pub in_copy: bool,
    /// The running copy is this site's own copy into the target shard,
    /// which drain adopts
    pub copy_into_target: bool,
    /// Another row with the same hash is active somewhere else.
    pub active_elsewhere: bool,
    pub offset: i32,
    pub node_override: Option<&'a str>,
}

pub fn classify(i: &Input) -> Action {
    let site = i.site;
    if i.in_copy && !i.copy_into_target {
        return Action::Blocked(
            "part of a copy that is still running; let it finish or cancel it".into(),
        );
    }
    if !site.active {
        return match site.assignment {
            Assignment::Unassigned if i.active_elsewhere => Action::Reclaim,
            Assignment::Unassigned => {
                Action::Blocked("inactive, and no other copy of this hash is active".into())
            }
            _ => Action::Blocked(
                "inactive but still assigned; unassign it or activate it first".into(),
            ),
        };
    }
    if site.assignment == Assignment::Unassigned && !i.current_or_pending {
        // graphman unused record would pick this up as it stands
        return Action::Reclaim;
    }

    let Some(detail) = i.detail else {
        return Action::Blocked("no deployment metadata in its shard".into());
    };
    let Some(head) = detail.head else {
        return Action::Blocked(
            "has not indexed any blocks, so graphman copy refuses it; remove it and redeploy \
             once the deployment rules point at the target shard"
                .into(),
        );
    };
    if head <= i.offset {
        return Action::Blocked(format!(
            "head is block {head}, graphman copy needs it above the offset {}; lower --offset \
             or let it index further",
            i.offset
        ));
    }
    if head - i.offset < detail.earliest_block {
        return Action::Blocked(format!(
            "head {head} minus offset {} is below its pruned earliest block {}; use \
             --offset {} or less",
            i.offset,
            detail.earliest_block,
            head - detail.earliest_block
        ));
    }

    let node = match (i.node_override, site.assignment.node()) {
        (Some(n), _) => n.to_string(),
        (None, Some(n)) => n.to_string(),
        (None, None) => {
            return Action::Blocked(
                "unassigned, so there is no node to index the copy; pass --node".into(),
            );
        }
    };
    Action::Move { node }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site(active: bool, assignment: Assignment) -> Site {
        Site {
            id: 7,
            hash: "QmA".into(),
            namespace: "sgd7".into(),
            shard: "primary".into(),
            active,
            assignment,
        }
    }

    fn detail(head: Option<i32>, earliest: i32) -> Detail {
        Detail {
            head,
            failed: false,
            fatal_error: None,
            synced: true,
            earliest_block: earliest,
        }
    }

    fn input<'a>(site: &'a Site, detail: Option<&'a Detail>) -> Input<'a> {
        Input {
            site,
            detail,
            current_or_pending: true,
            in_copy: false,
            copy_into_target: false,
            active_elsewhere: false,
            offset: 200,
            node_override: None,
        }
    }

    #[test]
    fn assigned_moves_to_its_own_node() {
        let s = site(true, Assignment::Assigned("idx0".into()));
        let d = detail(Some(5000), 0);
        assert_eq!(
            classify(&input(&s, Some(&d))),
            Action::Move {
                node: "idx0".into()
            }
        );
    }

    #[test]
    fn paused_moves_too() {
        let s = site(true, Assignment::Paused("idx1".into()));
        let d = detail(Some(5000), 0);
        assert_eq!(
            classify(&input(&s, Some(&d))),
            Action::Move {
                node: "idx1".into()
            }
        );
    }

    #[test]
    fn node_override_wins() {
        let s = site(true, Assignment::Assigned("idx0".into()));
        let d = detail(Some(5000), 0);
        let mut i = input(&s, Some(&d));
        i.node_override = Some("idx9");
        assert_eq!(
            classify(&i),
            Action::Move {
                node: "idx9".into()
            }
        );
    }

    #[test]
    fn unassigned_but_served_needs_a_node() {
        let s = site(true, Assignment::Unassigned);
        let d = detail(Some(5000), 0);
        assert!(
            matches!(classify(&input(&s, Some(&d))), Action::Blocked(m) if m.contains("--node"))
        );
        let mut i = input(&s, Some(&d));
        i.node_override = Some("idx0");
        assert_eq!(
            classify(&i),
            Action::Move {
                node: "idx0".into()
            }
        );
    }

    #[test]
    fn unassigned_and_unreferenced_is_reclaim() {
        let s = site(true, Assignment::Unassigned);
        let mut i = input(&s, None);
        i.current_or_pending = false;
        assert_eq!(classify(&i), Action::Reclaim);
    }

    #[test]
    fn inactive_leftover_is_reclaim_only_if_another_copy_is_active() {
        let s = site(false, Assignment::Unassigned);
        let mut i = input(&s, None);
        assert!(matches!(classify(&i), Action::Blocked(_)));
        i.active_elsewhere = true;
        assert_eq!(classify(&i), Action::Reclaim);
    }

    #[test]
    fn never_indexed_is_blocked() {
        let s = site(true, Assignment::Assigned("idx0".into()));
        let d = detail(None, 0);
        assert!(
            matches!(classify(&input(&s, Some(&d))), Action::Blocked(m) if m.contains("not indexed"))
        );
    }

    #[test]
    fn head_at_offset_is_blocked() {
        let s = site(true, Assignment::Assigned("idx0".into()));
        let d = detail(Some(200), 0);
        assert!(
            matches!(classify(&input(&s, Some(&d))), Action::Blocked(m) if m.contains("above the offset"))
        );
    }

    #[test]
    fn pruned_below_offset_suggests_offset() {
        let s = site(true, Assignment::Assigned("idx0".into()));
        let d = detail(Some(5000), 4900);
        assert!(
            matches!(classify(&input(&s, Some(&d))), Action::Blocked(m) if m.contains("--offset 100"))
        );
    }

    #[test]
    fn own_copy_into_target_is_adopted() {
        let s = site(true, Assignment::Unassigned);
        let d = detail(Some(5000), 0);
        let mut i = input(&s, Some(&d));
        i.in_copy = true;
        i.copy_into_target = true;
        i.node_override = Some("idx0");
        assert_eq!(
            classify(&i),
            Action::Move {
                node: "idx0".into()
            }
        );
    }

    #[test]
    fn running_copy_is_blocked() {
        let s = site(true, Assignment::Assigned("idx0".into()));
        let d = detail(Some(5000), 0);
        let mut i = input(&s, Some(&d));
        i.in_copy = true;
        assert!(matches!(classify(&i), Action::Blocked(_)));
    }
}
