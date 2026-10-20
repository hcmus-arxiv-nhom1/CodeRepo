import json

import pytest

from src.parser.gather import TexVersion, expand, gather, iter_version_dirs, main, strip_comments

DOC = "\\documentclass{article}\n\\title{%s}\n\\begin{document}\n%s\n\\end{document}\n"


def version(tmp_path, files, title=None):
    vdir = tmp_path / "2606-00001" / "v1"
    for rel, text in files.items():
        (vdir / rel).parent.mkdir(parents=True, exist_ok=True)
        (vdir / rel).write_text(text, encoding="utf-8", newline="\n")
    vdir.mkdir(parents=True, exist_ok=True)
    if title is not None:
        meta = {"arxiv_id": "2606.00001", "title": title}
        (vdir.parent / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    return vdir


def test_follows_input_and_include_transitively(tmp_path):
    vdir = version(
        tmp_path,
        {
            "main.tex": DOC % ("T", "\\input{sections/intro}\n\\include{sections/method.tex}"),
            "sections/intro.tex": "Intro. \\input sections/detail\n",
            "sections/detail.tex": "Detail.\n",
            "sections/method.tex": "Method.\n",
            "old/unused.tex": "Never included.\n",
        },
    )
    res = gather(vdir)
    assert res.root == "main.tex"
    assert res.files == [
        "main.tex",
        "sections/intro.tex",
        "sections/detail.tex",
        "sections/method.tex",
    ]
    assert res.unused == ["old/unused.tex"]
    assert res.missing == [] and res.other_roots == []


def test_commented_commands_are_not_followed(tmp_path):
    body = "% \\input{a}\n50\\% done \\input{b} % \\input{c}\nline\\\\% \\input{d}\n"
    vdir = version(tmp_path, {"main.tex": DOC % ("T", body), **{f"{x}.tex": x for x in "abcd"}})
    res = gather(vdir)
    assert res.files == ["main.tex", "b.tex"]  # "\%" is text, "\\%" starts a comment
    assert sorted(res.unused) == ["a.tex", "c.tex", "d.tex"]


def test_commented_documentclass_is_not_a_root(tmp_path):
    vdir = version(
        tmp_path, {"notes.tex": "% \\documentclass{article}\n", "main.tex": DOC % ("T", "")}
    )
    assert gather(vdir).root == "main.tex"


def test_cycles_terminate(tmp_path):
    vdir = version(
        tmp_path,
        {"main.tex": DOC % ("T", "\\input{a}"), "a.tex": "A \\input{b}", "b.tex": "B \\input{a}"},
    )
    assert gather(vdir).files == ["main.tex", "a.tex", "b.tex"]
    text = expand(vdir)
    assert text.count("A ") == 1 and "B \\input{a}" in text  # the cycle is left as written


def test_missing_targets_and_macro_arguments(tmp_path):
    body = "\\input{fig/plot.pgf}\n\\newcommand{\\sec}[1]{\\input{#1}}\n\\input{gone}"
    res = gather(version(tmp_path, {"main.tex": DOC % ("T", body)}))
    assert res.missing == ["fig/plot.pgf", "gone"]


def test_paths_resolve_from_root_folder_then_including_folder(tmp_path):
    vdir = version(
        tmp_path,
        {
            "paper/main.tex": DOC % ("T", "\\input{sec/a}"),
            "paper/sec/a.tex": "\\input{b}",  # relative to the including file (fallback)
            "paper/sec/b.tex": "B",
            "paper/../escape.tex": "x",
        },
    )
    assert gather(vdir).files == ["paper/main.tex", "paper/sec/a.tex", "paper/sec/b.tex"]
    assert TexVersion(vdir).resolve("../../etc/passwd", "paper/main.tex") is None


@pytest.mark.parametrize(
    ("files", "expected"),
    [
        # main paper + supplementary material, both complete documents
        (
            {"main.tex": DOC % ("T", "x" * 50), "supplementary.tex": DOC % ("T", "y" * 500)},
            "main.tex",
        ),
        ({"main.tex": DOC % ("T", ""), "main_SI.tex": DOC % ("T", "")}, "main.tex"),
        # ACL template leftover and a backup copy
        (
            {
                "acl_latex.tex": DOC % ("T", ""),
                "acl_lualatex.tex": DOC % ("T", "x" * 900),
                "backup/acl.tex": DOC % ("T", ""),
            },
            "acl_latex.tex",
        ),
        # a standalone figure included by the main file is not a root
        (
            {
                "fig.tex": "\\documentclass{standalone}\\begin{document}F\\end{document}",
                "main.tex": DOC % ("T", "\\input{fig}"),
            },
            "main.tex",
        ),
        # camel-case side document
        (
            {"Paper.tex": DOC % ("T", ""), "ReproducibilityChecklist.tex": DOC % ("T", "x" * 900)},
            "Paper.tex",
        ),
    ],
)
def test_root_choice_among_several_documentclass(tmp_path, files, expected):
    res = gather(version(tmp_path, files))
    assert res.root == expected
    assert sorted(res.other_roots) == sorted(set(files) - {expected})


def test_title_from_metadata_beats_a_bigger_unedited_template(tmp_path):
    files = {
        "conference_101719.tex": DOC % ("Conference Paper Title*\\\\", "x" * 2000),
        "fedlab_aaai.tex": DOC % ("FedLAB: Traceable \\textbf{Semantic} Codebooks", "y" * 100),
    }
    vdir = version(tmp_path, files, title="FedLAB: Traceable Semantic Codebooks")
    assert gather(vdir).root == "fedlab_aaai.tex"


def test_no_documentclass_means_no_root(tmp_path):
    res = gather(version(tmp_path, {"fragment.tex": "\\section{A}"}))
    assert res.root is None and res.unused == ["fragment.tex"]


def test_expand_inlines_files_and_keeps_comments(tmp_path):
    vdir = version(
        tmp_path,
        {
            "main.tex": DOC % ("T", "\\input{a} % \\input{a}\n\\input{fig.pgf}"),
            "a.tex": "Section A text.\n",
        },
    )
    text = expand(vdir)
    assert "Section A text.\n % \\input{a}" in text  # inlined, comment kept verbatim
    assert "\\input{fig.pgf}" in text  # unresolved: left as written
    assert text.startswith("\\documentclass{article}")


def test_crlf_files(tmp_path):
    vdir = version(tmp_path, {})
    (vdir / "main.tex").write_bytes(b"\\documentclass{article}\r\n\\input  sec % \\input{x}\r\n")
    (vdir / "sec.tex").write_bytes(b"S\r\n")
    assert gather(vdir).files == ["main.tex", "sec.tex"]


def test_latin1_file_is_read(tmp_path):
    vdir = version(tmp_path, {"main.tex": DOC % ("T", "\\input{a}")})
    (vdir / "a.tex").write_bytes("Schr\xf6dinger".encode("latin-1"))
    assert "Schrödinger" in expand(vdir)


def test_strip_comments():
    assert strip_comments("a % c\nb\\%c % d\n%e") == "a \nb\\%c \n"


def test_cli_over_a_corpus(tmp_path, capsys):
    version(tmp_path, {"main.tex": DOC % ("T", "\\input{a}"), "a.tex": "A"})
    (tmp_path / "2606-00001" / "v2").mkdir()  # PDF-only version: empty folder
    out = tmp_path / "gather.jsonl"
    assert [v.name for v in iter_version_dirs(tmp_path)] == ["v1", "v2"]
    assert main([str(tmp_path), "--jsonl", str(out)]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["versions"] == 2 and summary["no_tex"] == 1
    assert summary["with_root"] == summary["multi_file"] == 1
    assert summary["ambiguous_root"] == summary["no_root_with_tex"] == 0
    assert len(out.read_text(encoding="utf-8").splitlines()) == 2
