use std::{collections::BTreeMap, num::NonZeroU32};

use text::Rope;

const MAX_ANALYZED_BYTES: usize = 1024 * 1024;
const MAX_ANALYZED_LINES: usize = 10_000;
const MAX_INDENT_SIZE: u32 = 128;
const MAX_TAB_WIDTH: usize = 16;
const MAX_TAB_ALIGNMENT_LINES: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum IndentationKind {
    Space,
    Tab,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct InferredIndentation {
    pub hard_tabs: bool,
    pub tab_size: Option<NonZeroU32>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct IndentationAnalysis {
    kind: Option<IndentationKind>,
    space_size: Option<NonZeroU32>,
    tab_width_scores: [u16; MAX_TAB_WIDTH],
    tab_width_observations: [u16; MAX_TAB_WIDTH],
    tabbed_line_widths: Vec<[u32; MAX_TAB_WIDTH]>,
}

#[derive(Clone, Copy, Debug, Default)]
struct CandidateEvidence {
    votes: u32,
    weight: u32,
    first_seen: usize,
}

#[derive(Clone, Debug)]
struct LineInfo {
    text: String,
    indentation_kind: Option<IndentationKind>,
    indentation_count: u32,
    leading_tabs: u32,
    alignment_spaces: u32,
    content_start: usize,
}

impl IndentationAnalysis {
    pub(crate) fn new(text: &Rope) -> Self {
        let lines = collect_lines(text);
        let mut candidates = BTreeMap::new();
        let mut previous_line = None;
        let mut last_candidate = None;
        let mut candidate_index = 0;

        for line in &lines {
            if let Some(previous_line) = previous_line {
                if let Some(candidate) = indentation_change(previous_line, line) {
                    if candidate.1 <= MAX_INDENT_SIZE {
                        let evidence = candidates.entry(candidate).or_insert_with(|| {
                            let first_seen = candidate_index;
                            candidate_index += 1;
                            CandidateEvidence {
                                first_seen,
                                ..Default::default()
                            }
                        });
                        evidence.votes += 1;
                        evidence.weight += 1;
                        last_candidate = Some(candidate);
                    }
                } else if same_indentation(previous_line, line)
                    && let Some(candidate) = last_candidate
                    && let Some(evidence) = candidates.get_mut(&candidate)
                {
                    evidence.weight += 1;
                } else {
                    last_candidate = None;
                }
            }
            previous_line = Some(line);
        }

        let winning_candidate = confident_candidate(&candidates, false)
            .or_else(|| confident_candidate(&candidates, true));
        let mut analysis = Self::default();
        if let Some((kind, size)) = winning_candidate {
            analysis.kind = Some(kind);
            if kind == IndentationKind::Space {
                analysis.space_size = NonZeroU32::new(size);
            }
        }
        analysis.collect_tab_width_evidence(&lines);
        analysis
    }

    pub(crate) fn inferred_indentation(
        &self,
        configured_tab_size: NonZeroU32,
        preferred_line_length: u32,
    ) -> Option<InferredIndentation> {
        match self.kind? {
            IndentationKind::Space => Some(InferredIndentation {
                hard_tabs: false,
                tab_size: self.space_size,
            }),
            IndentationKind::Tab => Some(InferredIndentation {
                hard_tabs: true,
                tab_size: self.inferred_tab_width(configured_tab_size, preferred_line_length),
            }),
        }
    }

    fn collect_tab_width_evidence(&mut self, lines: &[LineInfo]) {
        let mut previous_meaningful_lines: Vec<&LineInfo> = Vec::new();

        for line in lines {
            if line.leading_tabs > 0 && line.alignment_spaces > 0 {
                if self.tabbed_line_widths.len() < MAX_TAB_ALIGNMENT_LINES {
                    self.tabbed_line_widths.push(std::array::from_fn(|index| {
                        visual_width(&line.text, (index + 1) as u32)
                    }));
                }

                for previous_line in previous_meaningful_lines.iter().rev().take(3) {
                    if let Some(opening_delimiter_end) =
                        last_unmatched_opening_delimiter(&previous_line.text)
                    {
                        for tab_width in 1..=MAX_TAB_WIDTH {
                            let tab_width = tab_width as u32;
                            if visual_column(&line.text, line.content_start, tab_width)
                                == visual_column(
                                    &previous_line.text,
                                    opening_delimiter_end,
                                    tab_width,
                                )
                            {
                                let index = tab_width as usize - 1;
                                self.tab_width_scores[index] =
                                    self.tab_width_scores[index].saturating_add(3);
                                self.tab_width_observations[index] =
                                    self.tab_width_observations[index].saturating_add(1);
                            }
                        }
                    }

                    if line.leading_tabs != previous_line.leading_tabs
                        && let (
                            Some((line_token, line_token_offset)),
                            Some((previous_token, previous_token_offset)),
                        ) = (alignment_token(line), alignment_token(previous_line))
                        && line_token == previous_token
                    {
                        for tab_width in 1..=MAX_TAB_WIDTH {
                            let tab_width = tab_width as u32;
                            if visual_column(&line.text, line_token_offset, tab_width)
                                == visual_column(
                                    &previous_line.text,
                                    previous_token_offset,
                                    tab_width,
                                )
                            {
                                let index = tab_width as usize - 1;
                                self.tab_width_scores[index] =
                                    self.tab_width_scores[index].saturating_add(1);
                                self.tab_width_observations[index] =
                                    self.tab_width_observations[index].saturating_add(1);
                            }
                        }
                    }
                }
            }

            previous_meaningful_lines.push(line);
        }
    }

    fn inferred_tab_width(
        &self,
        configured_tab_size: NonZeroU32,
        preferred_line_length: u32,
    ) -> Option<NonZeroU32> {
        let mut scores = self.tab_width_scores.map(i32::from);
        for widths in &self.tabbed_line_widths {
            if widths.iter().any(|width| *width <= preferred_line_length) {
                for (index, width) in widths.iter().enumerate() {
                    if *width > preferred_line_length {
                        scores[index] -= 1;
                    }
                }
            }
        }

        let configured_index = usize::try_from(configured_tab_size.get())
            .ok()
            .and_then(|size| size.checked_sub(1))
            .filter(|index| *index < MAX_TAB_WIDTH);
        let mut candidates = (0..MAX_TAB_WIDTH).collect::<Vec<_>>();
        candidates.sort_by_key(|index| {
            (
                scores[*index],
                self.tab_width_observations[*index],
                Some(*index) == configured_index,
            )
        });

        let winner = candidates.pop()?;
        let runner_up = candidates.last().copied();
        let winning_score = scores[winner];
        let runner_up_score = runner_up.map_or(0, |index| scores[index].max(0));
        if self.tab_width_observations[winner] < 3
            || winning_score < 3
            || winning_score < runner_up_score.saturating_mul(2)
        {
            return None;
        }

        NonZeroU32::new((winner + 1) as u32)
    }
}

fn collect_lines(text: &Rope) -> Vec<LineInfo> {
    let mut result = Vec::new();
    let mut bytes_analyzed: usize = 0;
    let mut lines = text.chunks().lines();
    while result.len() < MAX_ANALYZED_LINES {
        let Some(line) = lines.next() else {
            break;
        };
        if bytes_analyzed.saturating_add(line.len()) > MAX_ANALYZED_BYTES {
            break;
        }
        bytes_analyzed += line.len().saturating_add(1);
        if let Some(line) = line_info(line) {
            result.push(line);
        }
    }
    result
}

fn line_info(line: &str) -> Option<LineInfo> {
    let mut leading_tabs = 0;
    let mut alignment_spaces = 0;
    let mut leading_spaces = 0;
    let mut content_start = 0;
    let mut saw_tab = false;

    for (offset, character) in line.char_indices() {
        match character {
            '\t' if leading_spaces == 0 && alignment_spaces == 0 => {
                saw_tab = true;
                leading_tabs += 1;
                content_start = offset + character.len_utf8();
            }
            ' ' if saw_tab => {
                alignment_spaces += 1;
                content_start = offset + character.len_utf8();
            }
            ' ' if !saw_tab => {
                leading_spaces += 1;
                content_start = offset + character.len_utf8();
            }
            '\t' | ' ' => return None,
            _ => break,
        }
    }

    if line[content_start..].trim().is_empty() {
        return None;
    }

    let (indentation_kind, indentation_count) = if leading_tabs > 0 {
        (Some(IndentationKind::Tab), leading_tabs)
    } else if leading_spaces > 0 {
        (Some(IndentationKind::Space), leading_spaces)
    } else {
        (None, 0)
    };

    Some(LineInfo {
        text: line.to_owned(),
        indentation_kind,
        indentation_count,
        leading_tabs,
        alignment_spaces,
        content_start,
    })
}

fn indentation_change(previous_line: &LineInfo, line: &LineInfo) -> Option<(IndentationKind, u32)> {
    match (previous_line.indentation_kind, line.indentation_kind) {
        (Some(previous_kind), Some(kind)) if previous_kind == kind => {
            let difference = previous_line
                .indentation_count
                .abs_diff(line.indentation_count);
            (difference > 0).then_some((kind, difference))
        }
        (None, Some(kind)) => Some((kind, line.indentation_count)),
        (Some(kind), None) => Some((kind, previous_line.indentation_count)),
        _ => None,
    }
}

fn same_indentation(previous_line: &LineInfo, line: &LineInfo) -> bool {
    previous_line.indentation_kind == line.indentation_kind
        && previous_line.indentation_count == line.indentation_count
        && line.indentation_kind.is_some()
}

fn confident_candidate(
    candidates: &BTreeMap<(IndentationKind, u32), CandidateEvidence>,
    include_single_space: bool,
) -> Option<(IndentationKind, u32)> {
    let mut candidates = candidates
        .iter()
        .filter(|((kind, size), _)| {
            include_single_space || *kind == IndentationKind::Tab || *size > 1
        })
        .collect::<Vec<_>>();
    let total_weight = candidates
        .iter()
        .map(|(_, evidence)| evidence.weight)
        .sum::<u32>();
    candidates.sort_by_key(|(_, evidence)| {
        (
            evidence.weight,
            evidence.votes,
            usize::MAX - evidence.first_seen,
        )
    });
    let (candidate, evidence) = candidates.pop()?;
    let runner_up_weight = candidates.last().map_or(0, |(_, evidence)| evidence.weight);

    if evidence.weight < 2
        || evidence.weight.saturating_mul(100) < total_weight.saturating_mul(60)
        || evidence
            .weight
            .saturating_sub(runner_up_weight)
            .saturating_mul(100)
            < total_weight.saturating_mul(20)
    {
        return None;
    }
    Some(*candidate)
}

fn last_unmatched_opening_delimiter(line: &str) -> Option<usize> {
    let mut delimiters = Vec::new();
    for (offset, character) in line.char_indices() {
        match character {
            '(' | '[' => delimiters.push((character, offset + character.len_utf8())),
            ')' => {
                if delimiters
                    .last()
                    .is_some_and(|(opening, _)| *opening == '(')
                {
                    delimiters.pop();
                }
            }
            ']' => {
                if delimiters
                    .last()
                    .is_some_and(|(opening, _)| *opening == '[')
                {
                    delimiters.pop();
                }
            }
            _ => {}
        }
    }
    delimiters.last().map(|(_, offset)| *offset)
}

fn alignment_token(line: &LineInfo) -> Option<(char, usize)> {
    line.text[line.content_start..]
        .char_indices()
        .find(|(_, character)| matches!(character, '=' | ':' | '|' | '.'))
        .map(|(offset, character)| (character, line.content_start + offset))
}

fn visual_column(line: &str, end_offset: usize, tab_width: u32) -> u32 {
    let mut column = 0u32;
    for character in line[..end_offset].chars() {
        if character == '\t' {
            column += tab_width - column % tab_width;
        } else {
            column += 1;
        }
    }
    column
}

fn visual_width(line: &str, tab_width: u32) -> u32 {
    visual_column(line, line.len(), tab_width)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn infer(text: &str, configured_tab_size: u32) -> Option<InferredIndentation> {
        IndentationAnalysis::new(&Rope::from(text)).inferred_indentation(
            NonZeroU32::new(configured_tab_size).expect("test tab size must be non-zero"),
            80,
        )
    }

    #[test]
    fn infers_space_indentation_sizes() {
        for size in [1, 2, 3, 4, 8] {
            let indent = " ".repeat(size);
            let text =
                format!("root\n{indent}child\n{indent}{indent}grandchild\n{indent}child\nroot\n");
            assert_eq!(
                infer(&text, 7),
                Some(InferredIndentation {
                    hard_tabs: false,
                    tab_size: NonZeroU32::new(size as u32),
                })
            );
        }
    }

    #[test]
    fn rejects_ambiguous_or_insufficient_space_indentation() {
        assert_eq!(infer("root\n  child\nroot\n    child\nroot\n", 7), None);
        assert_eq!(infer("root\n   child\n", 7), None);
        assert_eq!(infer("", 7), None);
    }

    #[test]
    fn repeated_siblings_confirm_an_indentation_candidate() {
        assert_eq!(
            infer("root\n    first\n    second\n", 7),
            Some(InferredIndentation {
                hard_tabs: false,
                tab_size: NonZeroU32::new(4),
            })
        );
    }

    #[test]
    fn ignores_single_space_alignment_when_larger_indentation_exists() {
        let text = "root\n    child\n     aligned\n    child\nroot\n";
        assert_eq!(
            infer(text, 7),
            Some(InferredIndentation {
                hard_tabs: false,
                tab_size: NonZeroU32::new(4),
            })
        );
    }

    #[test]
    fn infers_tab_style_without_guessing_unsubstantiated_width() {
        assert_eq!(
            infer("root\n\tchild\n\t\tgrandchild\n\tchild\nroot\n", 7),
            Some(InferredIndentation {
                hard_tabs: true,
                tab_size: None,
            })
        );
    }

    #[test]
    fn infers_tab_width_from_continuation_alignment() {
        let text = concat!(
            "root\n",
            "\tchild\n",
            "\t\tgrandchild\n",
            "\tchild\n",
            "root\n",
            "call(thing,\n",
            "\t value)\n",
            "call(thing,\n",
            "\t value)\n",
            "call(thing,\n",
            "\t value)\n",
        );
        assert_eq!(
            infer(text, 7),
            Some(InferredIndentation {
                hard_tabs: true,
                tab_size: NonZeroU32::new(4),
            })
        );
    }
}
