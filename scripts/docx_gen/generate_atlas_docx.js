#!/usr/bin/env node
/**
 * Chart Atlas — Word Document Generator
 * ======================================
 * Reads all chart_atlas_*.md files from reports/chart_atlas/
 * and produces a single, well-formatted .docx with sections per page.
 *
 * Usage:
 *   node scripts/docx_gen/generate_atlas_docx.js
 *
 * Output:
 *   reports/Chart_Atlas_Complete.docx
 */

const fs = require("fs");
const path = require("path");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, HeadingLevel, BorderStyle, WidthType,
  ShadingType, PageNumber, PageBreak, LevelFormat, TableOfContents,
} = require("docx");

const ROOT = path.resolve(__dirname, "../..");
const ATLAS_DIR = path.join(ROOT, "reports", "chart_atlas");
const OUTPUT = path.join(ROOT, "reports", "Chart_Atlas_Complete.docx");

// Page order
const PAGE_ORDER = [
  "chart_atlas_home.md",
  "chart_atlas_overview.md",
  "chart_atlas_earnings.md",
  "chart_atlas_stocks.md",
  "chart_atlas_editorial.md",
  "chart_atlas_genie.md",
  "chart_atlas_country.md",
];

const PAGE_LABELS = {
  "chart_atlas_home.md": "Home (Welcome)",
  "chart_atlas_overview.md": "Overview",
  "chart_atlas_earnings.md": "Earnings",
  "chart_atlas_stocks.md": "Stocks",
  "chart_atlas_editorial.md": "Editorial",
  "chart_atlas_genie.md": "Genie (AI Assistant)",
  "chart_atlas_country.md": "Country Deep Dive",
};

// ── Parse one MD file into structured data ─────────────────────────
function parseMd(content, filename) {
  const label = PAGE_LABELS[filename] || filename;
  const snippets = [];

  // Extract description line (> blockquote after ----)
  const descMatch = content.match(/^>\s*(.+)$/m);
  const description = descMatch ? descMatch[1].trim() : "";

  // Extract source line
  const srcMatch = content.match(/\*\*Source:\*\*\s*`([^`]+)`\s*\(([^)]+)\)/);
  const source = srcMatch ? `${srcMatch[1]} (${srcMatch[2]})` : "";

  // Extract generated line
  const genMatch = content.match(/\*\*Generated:\*\*\s*(.+?)(?:\n|$)/);
  const generated = genMatch ? genMatch[1].trim() : "";

  // Split into snippet blocks: ## `function_name()` sections
  const sections = content.split(/^## /m).slice(1); // skip header

  for (const section of sections) {
    // Skip "Index" section
    if (section.startsWith("Index")) continue;

    // Get title line
    const titleLine = section.split("\n")[0].trim();

    // Extract function name and tags
    const nameMatch = titleLine.match(/`([^`]+)`\s*[—–-]\s*(.+)/);
    if (!nameMatch) continue;
    const funcName = nameMatch[1];
    const tags = nameMatch[2].trim();

    // Extract line info
    const lineMatch = section.match(/Lines?\s*(\d+)[–-](\d+)\s*(?:\((\d+)\s*lines?\))?/);
    const lineInfo = lineMatch ? `Lines ${lineMatch[1]}–${lineMatch[2]}` : "";
    const lineCount = lineMatch && lineMatch[3] ? `${lineMatch[3]} lines` : "";

    // Extract code block
    const codeMatch = section.match(/```python\n([\s\S]*?)```/);
    const code = codeMatch ? codeMatch[1].trimEnd() : "";

    if (code) {
      snippets.push({ funcName, tags, lineInfo, lineCount, code });
    }
  }

  return { label, description, source, generated, snippets };
}

// ── Build code block as a shaded table cell ────────────────────────
function makeCodeBlock(code) {
  const lines = code.split("\n");
  const codeParas = lines.map(
    (line) =>
      new Paragraph({
        spacing: { before: 0, after: 0, line: 260 },
        children: [
          new TextRun({
            text: line || " ",
            font: "Courier New",
            size: 15, // 7.5pt
            color: "1F2937",
          }),
        ],
      })
  );

  return new Table({
    width: { size: 9360, type: WidthType.DXA },
    columnWidths: [9360],
    rows: [
      new TableRow({
        children: [
          new TableCell({
            width: { size: 9360, type: WidthType.DXA },
            shading: { fill: "F3F4F6", type: ShadingType.CLEAR },
            borders: {
              top: { style: BorderStyle.SINGLE, size: 1, color: "D1D5DB" },
              bottom: { style: BorderStyle.SINGLE, size: 1, color: "D1D5DB" },
              left: { style: BorderStyle.SINGLE, size: 6, color: "3B82F6" },
              right: { style: BorderStyle.SINGLE, size: 1, color: "D1D5DB" },
            },
            margins: { top: 100, bottom: 100, left: 160, right: 160 },
            children: codeParas,
          }),
        ],
      }),
    ],
  });
}

// ── Build the document ─────────────────────────────────────────────
async function main() {
  // Read and parse all atlas files
  const pages = [];
  for (const file of PAGE_ORDER) {
    const fp = path.join(ATLAS_DIR, file);
    if (!fs.existsSync(fp)) continue;
    const content = fs.readFileSync(fp, "utf-8");
    const parsed = parseMd(content, file);
    if (parsed.snippets.length > 0) {
      pages.push(parsed);
    }
  }

  if (pages.length === 0) {
    console.error("No chart atlas MDs found. Run generate_chart_atlas.py first.");
    process.exit(1);
  }

  const totalSnippets = pages.reduce((s, p) => s + p.snippets.length, 0);
  const now = new Date().toISOString().slice(0, 16).replace("T", " ");

  // ── Title page section ───────────────────────────────────────────
  const titleSection = {
    properties: {
      page: {
        size: { width: 12240, height: 15840 },
        margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 },
      },
    },
    children: [
      new Paragraph({ spacing: { before: 3600 } }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { after: 200 },
        children: [
          new TextRun({
            text: "CHART ATLAS",
            bold: true,
            size: 56,
            font: "Arial",
            color: "1E3A5F",
          }),
        ],
      }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { after: 100 },
        children: [
          new TextRun({
            text: "The Attention Economy Dashboard",
            size: 28,
            font: "Arial",
            color: "6B7280",
          }),
        ],
      }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { after: 60 },
        children: [
          new TextRun({
            text: "Complete Visualisation Reference",
            size: 24,
            font: "Arial",
            color: "9CA3AF",
          }),
        ],
      }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        border: {
          top: { style: BorderStyle.SINGLE, size: 4, color: "3B82F6", space: 12 },
        },
        spacing: { before: 600, after: 200 },
        children: [
          new TextRun({
            text: `${pages.length} pages  \u00B7  ${totalSnippets} visualisations`,
            size: 22,
            font: "Arial",
            color: "374151",
          }),
        ],
      }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        spacing: { after: 100 },
        children: [
          new TextRun({
            text: `Generated: ${now}`,
            size: 20,
            font: "Arial",
            color: "9CA3AF",
          }),
        ],
      }),
      new Paragraph({
        alignment: AlignmentType.CENTER,
        children: [
          new TextRun({
            text: "Auto-generated from live source code",
            size: 18,
            font: "Arial",
            color: "9CA3AF",
            italics: true,
          }),
        ],
      }),
    ],
  };

  // ── TOC section ──────────────────────────────────────────────────
  const tocChildren = [
    new Paragraph({ children: [new PageBreak()] }),
    new Paragraph({
      heading: HeadingLevel.HEADING_1,
      children: [new TextRun({ text: "Table of Contents", bold: true, font: "Arial", size: 32 })],
    }),
    new TableOfContents("Table of Contents", {
      hyperlink: true,
      headingStyleRange: "1-3",
    }),
  ];

  // ── Page sections ────────────────────────────────────────────────
  const pageSections = [];

  for (const page of pages) {
    const children = [];

    // Page break before each page section
    children.push(new Paragraph({ children: [new PageBreak()] }));

    // Page heading
    children.push(
      new Paragraph({
        heading: HeadingLevel.HEADING_1,
        spacing: { after: 120 },
        children: [
          new TextRun({
            text: page.label,
            bold: true,
            font: "Arial",
            size: 36,
            color: "1E3A5F",
          }),
        ],
      })
    );

    // Source + description
    if (page.source) {
      children.push(
        new Paragraph({
          spacing: { after: 60 },
          children: [
            new TextRun({ text: "Source: ", bold: true, font: "Arial", size: 20, color: "374151" }),
            new TextRun({ text: page.source, font: "Courier New", size: 18, color: "6B7280" }),
          ],
        })
      );
    }
    if (page.description) {
      children.push(
        new Paragraph({
          spacing: { after: 200 },
          children: [
            new TextRun({
              text: page.description,
              font: "Arial",
              size: 20,
              color: "6B7280",
              italics: true,
            }),
          ],
        })
      );
    }

    // Summary table: list all snippets
    children.push(
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        spacing: { before: 200, after: 120 },
        children: [
          new TextRun({
            text: `Visualisations (${page.snippets.length})`,
            bold: true,
            font: "Arial",
            size: 26,
            color: "374151",
          }),
        ],
      })
    );

    // Index table
    const headerBorder = { style: BorderStyle.SINGLE, size: 1, color: "D1D5DB" };
    const borders = { top: headerBorder, bottom: headerBorder, left: headerBorder, right: headerBorder };
    const indexRows = [
      new TableRow({
        children: [
          new TableCell({
            width: { size: 400, type: WidthType.DXA },
            borders,
            shading: { fill: "1E3A5F", type: ShadingType.CLEAR },
            margins: { top: 60, bottom: 60, left: 80, right: 80 },
            children: [new Paragraph({ children: [new TextRun({ text: "#", bold: true, font: "Arial", size: 18, color: "FFFFFF" })] })],
          }),
          new TableCell({
            width: { size: 4400, type: WidthType.DXA },
            borders,
            shading: { fill: "1E3A5F", type: ShadingType.CLEAR },
            margins: { top: 60, bottom: 60, left: 80, right: 80 },
            children: [new Paragraph({ children: [new TextRun({ text: "Function", bold: true, font: "Arial", size: 18, color: "FFFFFF" })] })],
          }),
          new TableCell({
            width: { size: 2860, type: WidthType.DXA },
            borders,
            shading: { fill: "1E3A5F", type: ShadingType.CLEAR },
            margins: { top: 60, bottom: 60, left: 80, right: 80 },
            children: [new Paragraph({ children: [new TextRun({ text: "Type", bold: true, font: "Arial", size: 18, color: "FFFFFF" })] })],
          }),
          new TableCell({
            width: { size: 1700, type: WidthType.DXA },
            borders,
            shading: { fill: "1E3A5F", type: ShadingType.CLEAR },
            margins: { top: 60, bottom: 60, left: 80, right: 80 },
            children: [new Paragraph({ children: [new TextRun({ text: "Location", bold: true, font: "Arial", size: 18, color: "FFFFFF" })] })],
          }),
        ],
      }),
    ];

    page.snippets.forEach((s, i) => {
      const rowShading = i % 2 === 0 ? "FFFFFF" : "F9FAFB";
      indexRows.push(
        new TableRow({
          children: [
            new TableCell({
              width: { size: 400, type: WidthType.DXA },
              borders,
              shading: { fill: rowShading, type: ShadingType.CLEAR },
              margins: { top: 40, bottom: 40, left: 80, right: 80 },
              children: [new Paragraph({ children: [new TextRun({ text: `${i + 1}`, font: "Arial", size: 18, color: "6B7280" })] })],
            }),
            new TableCell({
              width: { size: 4400, type: WidthType.DXA },
              borders,
              shading: { fill: rowShading, type: ShadingType.CLEAR },
              margins: { top: 40, bottom: 40, left: 80, right: 80 },
              children: [new Paragraph({ children: [new TextRun({ text: s.funcName, font: "Courier New", size: 17, color: "1F2937" })] })],
            }),
            new TableCell({
              width: { size: 2860, type: WidthType.DXA },
              borders,
              shading: { fill: rowShading, type: ShadingType.CLEAR },
              margins: { top: 40, bottom: 40, left: 80, right: 80 },
              children: [new Paragraph({ children: [new TextRun({ text: s.tags, font: "Arial", size: 17, color: "6B7280" })] })],
            }),
            new TableCell({
              width: { size: 1700, type: WidthType.DXA },
              borders,
              shading: { fill: rowShading, type: ShadingType.CLEAR },
              margins: { top: 40, bottom: 40, left: 80, right: 80 },
              children: [new Paragraph({ children: [new TextRun({ text: s.lineInfo, font: "Arial", size: 17, color: "9CA3AF" })] })],
            }),
          ],
        })
      );
    });

    children.push(
      new Table({
        width: { size: 9360, type: WidthType.DXA },
        columnWidths: [400, 4400, 2860, 1700],
        rows: indexRows,
      })
    );

    // Each snippet
    for (let i = 0; i < page.snippets.length; i++) {
      const s = page.snippets[i];

      children.push(
        new Paragraph({
          heading: HeadingLevel.HEADING_3,
          spacing: { before: 360, after: 80 },
          children: [
            new TextRun({
              text: `${i + 1}. ${s.funcName}`,
              bold: true,
              font: "Arial",
              size: 24,
              color: "1F2937",
            }),
          ],
        })
      );

      // Tags + line info
      children.push(
        new Paragraph({
          spacing: { after: 100 },
          children: [
            new TextRun({ text: s.tags, font: "Arial", size: 18, color: "3B82F6", bold: true }),
            new TextRun({ text: `  \u00B7  ${s.lineInfo}`, font: "Arial", size: 18, color: "9CA3AF" }),
            ...(s.lineCount ? [new TextRun({ text: `  (${s.lineCount})`, font: "Arial", size: 18, color: "9CA3AF" })] : []),
          ],
        })
      );

      // Code block
      children.push(makeCodeBlock(s.code));
    }

    pageSections.push(...children);
  }

  // ── Assemble document ────────────────────────────────────────────
  const doc = new Document({
    styles: {
      default: {
        document: { run: { font: "Arial", size: 22 } },
      },
      paragraphStyles: [
        {
          id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 36, bold: true, font: "Arial", color: "1E3A5F" },
          paragraph: { spacing: { before: 240, after: 240 }, outlineLevel: 0 },
        },
        {
          id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 28, bold: true, font: "Arial", color: "374151" },
          paragraph: { spacing: { before: 200, after: 160 }, outlineLevel: 1 },
        },
        {
          id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 24, bold: true, font: "Arial", color: "1F2937" },
          paragraph: { spacing: { before: 160, after: 120 }, outlineLevel: 2 },
        },
      ],
    },
    sections: [
      titleSection,
      {
        properties: {
          page: {
            size: { width: 12240, height: 15840 },
            margin: { top: 1080, right: 1440, bottom: 1080, left: 1440 },
          },
        },
        headers: {
          default: new Header({
            children: [
              new Paragraph({
                alignment: AlignmentType.RIGHT,
                border: {
                  bottom: { style: BorderStyle.SINGLE, size: 2, color: "D1D5DB", space: 4 },
                },
                children: [
                  new TextRun({ text: "Chart Atlas", font: "Arial", size: 16, color: "9CA3AF", italics: true }),
                  new TextRun({ text: "  \u00B7  The Attention Economy", font: "Arial", size: 16, color: "D1D5DB" }),
                ],
              }),
            ],
          }),
        },
        footers: {
          default: new Footer({
            children: [
              new Paragraph({
                alignment: AlignmentType.CENTER,
                children: [
                  new TextRun({ text: "Page ", font: "Arial", size: 16, color: "9CA3AF" }),
                  new TextRun({ children: [PageNumber.CURRENT], font: "Arial", size: 16, color: "9CA3AF" }),
                ],
              }),
            ],
          }),
        },
        children: [...tocChildren, ...pageSections],
      },
    ],
  });

  const buffer = await Packer.toBuffer(doc);
  fs.writeFileSync(OUTPUT, buffer);
  console.log(`[atlas-docx] Written: ${OUTPUT}`);
  console.log(`[atlas-docx] ${pages.length} pages, ${totalSnippets} visualisation snippets`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
