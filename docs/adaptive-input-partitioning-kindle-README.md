# Adaptive Input Partitioning Design Document - Kindle Conversion Guide

This directory contains the Adaptive Input Partitioning design document in multiple formats optimized for reading on Kindle devices and e-readers.

## Available Formats

| Format | File | Best For |
|--------|------|----------|
| **Original Markdown** | `adaptive-input-partitioning-design.md` | GitHub, text editors, VSCode |
| **Kindle Markdown** | `adaptive-input-partitioning-design-kindle.md` | Pandoc conversion to EPUB/MOBI |
| **Kindle HTML** | `adaptive-input-partitioning-design-kindle.html` | Direct viewing or Calibre conversion |

## Quick Start

### Option 1: Send HTML Directly (Easiest)

1. Open `adaptive-input-partitioning-design-kindle.html` in your browser
2. Use browser's "Send to Kindle" extension
3. Or save as PDF and email to your Kindle

### Option 2: Convert to MOBI with Calibre (Recommended)

```bash
# Install Calibre (if not already installed)
# macOS
brew install --cask calibre

# Ubuntu/Debian
sudo apt-get install calibre

# Convert HTML to MOBI
ebook-convert \
  adaptive-input-partitioning-design-kindle.html \
  adaptive-input-partitioning.mobi \
  --title "Adaptive Input Partitioning - Spark Design" \
  --authors "Apache Spark Team" \
  --book-producer "Apache Spark" \
  --language en \
  --chapter "//*[@class='page-break']" \
  --margin-left 5 \
  --margin-right 5 \
  --margin-top 5 \
  --margin-bottom 5
```

Then email the `.mobi` file to your Kindle email address.

### Option 3: Convert to EPUB with Pandoc

```bash
# Install Pandoc
# macOS
brew install pandoc

# Ubuntu/Debian
sudo apt-get install pandoc

# Convert Markdown to EPUB
pandoc adaptive-input-partitioning-design-kindle.md \
  -o adaptive-input-partitioning.epub \
  --toc \
  --toc-depth=3 \
  --metadata title="Adaptive Input Partitioning Design" \
  --metadata author="Apache Spark Team" \
  --metadata language=en \
  --metadata date="2025-10-27" \
  --epub-cover-image=cover.png \
  --css=kindle.css

# Then convert EPUB to MOBI with Calibre or Kindle Previewer
ebook-convert \
  adaptive-input-partitioning.epub \
  adaptive-input-partitioning.mobi
```

## Sending to Kindle

### Method 1: Email to Kindle

1. **Find Your Kindle Email:**
   - Go to [Amazon Manage Your Content and Devices](https://www.amazon.com/mycd)
   - Click "Preferences" tab
   - Scroll to "Personal Document Settings"
   - Your Kindle email: `yourname_123@kindle.com`

2. **Approve Your Sender Email:**
   - In "Personal Document Settings"
   - Add your email to "Approved Personal Document E-mail List"

3. **Send the Document:**
   - From your approved email
   - To: `yourname_123@kindle.com`
   - Subject: `Convert` (optional, for EPUB/HTML → AZW3 conversion)
   - Attach: `.mobi`, `.epub`, or `.html` file
   - Wait 5-10 minutes for delivery

### Method 2: USB Transfer (Kindle E-Readers)

1. Connect Kindle to computer via USB
2. Copy `.mobi` or `.azw3` file to `Documents` folder
3. Eject Kindle safely
4. Document appears in your library

### Method 3: Send to Kindle Apps

For Kindle apps (iOS, Android, Mac, PC):
1. Use Send to Kindle app or browser extension
2. Send `.epub` or `.pdf` files
3. Syncs across all devices

## Advanced Customization

### Customize CSS Styling

Edit the `<style>` section in the HTML file to adjust:

```css
/* Font size */
body {
    font-size: 1.1em;  /* Increase for larger text */
}

/* Code block size */
pre code {
    font-size: 0.8em;  /* Decrease for more code on screen */
}

/* Page width */
body {
    max-width: 90%;  /* Adjust for wider/narrower layout */
}
```

### Create Custom Cover Image

```bash
# Create cover.png (recommended: 1600x2400px)
# Then add to EPUB:
pandoc adaptive-input-partitioning-design-kindle.md \
  -o adaptive-input-partitioning.epub \
  --epub-cover-image=cover.png \
  # ... other options
```

### Split into Multiple Books

For easier reading, split into separate books:

**Book 1: Overview & Analysis**
- Sections 1-2: Overview and Current Implementation

**Book 2: Design & Implementation**
- Sections 3-6: Proposed Solution through Implementation

**Book 3: Testing & Future**
- Sections 7-11: Testing, Alternatives, Future Work

Extract sections using Pandoc with `--extract` or manually edit the markdown.

## Troubleshooting

### Issue: Images Not Displaying

**Solution:** Kindle has limited image support. Convert images to:
- Format: JPG or PNG
- Max size: 5MB per image
- Resolution: 300 DPI or less

### Issue: Code Blocks Too Wide

**Solution 1:** Reduce font size in HTML CSS:
```css
pre code {
    font-size: 0.75em;
}
```

**Solution 2:** Enable word wrap in Calibre:
```bash
ebook-convert input.html output.mobi --flow-size 0
```

### Issue: Table of Contents Not Working

**Solution:** Ensure proper heading hierarchy (h1 → h2 → h3) and use Pandoc with `--toc`:
```bash
pandoc input.md -o output.epub --toc --toc-depth=3
```

### Issue: File Size Too Large

**Solution:** Compress images or split document:
```bash
# Optimize images
mogrify -quality 80 -resize 1200x *.png

# Split document
pandoc input.md -o output.epub --split-level=2
```

## Format Comparison

| Feature | HTML | EPUB | MOBI/AZW3 | PDF |
|---------|------|------|-----------|-----|
| **File Size** | Small | Medium | Medium | Large |
| **Reflowable** | Yes | Yes | Yes | No |
| **TOC Support** | Links | Native | Native | Links |
| **Code Blocks** | Good | Good | Fair | Excellent |
| **Tables** | Good | Fair | Fair | Excellent |
| **Images** | Excellent | Good | Good | Excellent |
| **Font Control** | CSS | CSS | Limited | Fixed |
| **Kindle Support** | Email | Email/USB | Email/USB | Email/USB |

**Recommendation:**
- **Best quality:** MOBI/AZW3 (via Calibre from HTML)
- **Best compatibility:** EPUB (via Pandoc from Markdown)
- **Quickest:** HTML (email directly)

## Testing Your Conversion

### Kindle Previewer (Desktop)

Download from [Amazon Kindle Previewer](https://www.amazon.com/Kindle-Previewer/b?node=21381691011)

```bash
# Open your file
kindle-previewer adaptive-input-partitioning.epub

# Test on different devices
# - Kindle Paperwhite
# - Kindle Oasis
# - Kindle Fire
# - Kindle App
```

### Calibre E-book Viewer

```bash
# View before sending
ebook-viewer adaptive-input-partitioning.mobi

# Edit metadata
ebook-meta adaptive-input-partitioning.mobi \
  --title="Adaptive Input Partitioning" \
  --authors="Spark Team" \
  --tags="Spark, AQE, Performance"
```

## Batch Conversion Script

Save as `convert-to-kindle.sh`:

```bash
#!/bin/bash

INPUT_MD="adaptive-input-partitioning-design-kindle.md"
INPUT_HTML="adaptive-input-partitioning-design-kindle.html"
OUTPUT_BASE="adaptive-input-partitioning"

echo "Converting to multiple formats..."

# EPUB from Markdown
pandoc "$INPUT_MD" \
  -o "${OUTPUT_BASE}.epub" \
  --toc --toc-depth=3 \
  --metadata title="Adaptive Input Partitioning" \
  --metadata author="Apache Spark Team" \
  --css=kindle.css

# MOBI from HTML (best quality)
ebook-convert "$INPUT_HTML" \
  "${OUTPUT_BASE}.mobi" \
  --title "Adaptive Input Partitioning" \
  --authors "Apache Spark Team" \
  --language en

# AZW3 from EPUB
ebook-convert "${OUTPUT_BASE}.epub" \
  "${OUTPUT_BASE}.azw3" \
  --enable-heuristics

# PDF for desktop reading
pandoc "$INPUT_MD" \
  -o "${OUTPUT_BASE}.pdf" \
  --toc --toc-depth=3 \
  --pdf-engine=xelatex \
  -V geometry:margin=1in

echo "Conversion complete!"
echo "Files created:"
ls -lh ${OUTPUT_BASE}.*
```

Run with:
```bash
chmod +x convert-to-kindle.sh
./convert-to-kindle.sh
```

## Optional Enhancements

### Add Syntax Highlighting

For code blocks with syntax highlighting:

```bash
# Install Pygments
pip install Pygments

# Convert with highlighting
pandoc input.md -o output.epub \
  --highlight-style=tango \
  --syntax-definition=scala.xml
```

### Add Footnotes

In markdown, use:
```markdown
This is a statement[^1] with a footnote.

[^1]: This is the footnote text.
```

### Add Cross-References

```markdown
See [Section 3](#proposed-solution) for details.

## Proposed Solution {#proposed-solution}
Content here...
```

## Resources

### Tools

- **Pandoc**: https://pandoc.org/
- **Calibre**: https://calibre-ebook.com/
- **Kindle Previewer**: https://www.amazon.com/Kindle-Previewer/b?node=21381691011
- **Kindle Create**: https://www.amazon.com/Kindle-Create/b?node=18292298011

### Documentation

- **Pandoc Manual**: https://pandoc.org/MANUAL.html
- **Calibre User Manual**: https://manual.calibre-ebook.com/
- **EPUB Spec**: https://www.w3.org/publishing/epub3/
- **Kindle Publishing Guidelines**: https://kdp.amazon.com/en_US/help/topic/G200735480

### Communities

- **Pandoc Discuss**: https://github.com/jgm/pandoc/discussions
- **Calibre Forum**: https://www.mobileread.com/forums/forumdisplay.php?f=166
- **r/kindle**: https://reddit.com/r/kindle

## FAQ

### Q: Which format should I use?

**A:** For best quality on Kindle, use MOBI created from HTML via Calibre. For cross-platform (iPad, Kobo, etc.), use EPUB.

### Q: Why does my code look wrong?

**A:** Kindle has limited monospace font support. Try reducing code block font size or breaking long lines.

### Q: Can I read this on my phone?

**A:** Yes! Use Kindle app (iOS/Android) and send via email or use Send to Kindle browser extension.

### Q: How do I update the document?

**A:** Re-convert and email to Kindle. Amazon will replace the old version with the new one (same title).

### Q: Can I share with others?

**A:** Yes, but be aware of licensing. Apache License 2.0 allows sharing with attribution.

## Support

For issues or questions:
1. Check [Calibre FAQ](https://manual.calibre-ebook.com/faq.html)
2. Check [Pandoc FAQ](https://pandoc.org/faqs.html)
3. Open issue in Spark repository
4. Ask on Spark mailing list

---

**Last Updated:** 2025-10-27
**Document Version:** 1.0
**Maintained By:** Apache Spark Documentation Team
