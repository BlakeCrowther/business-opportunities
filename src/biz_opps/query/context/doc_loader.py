from typing import Dict, Optional
import os

from biz_opps.utils.file import get_root_dir


class DocumentationLoader:
    """Loads and manages documents to use in LLM context."""

    def __init__(
        self,
        docs_dir: str = os.path.join(get_root_dir(), "docs"),
        verbose: bool = False,
    ):
        """
        Initialize text loader.

        Args:
            docs_dir: Directory containing documentation files.
            verbose: Whether to print verbose output.
        """
        self.docs_dir = docs_dir
        self.context_docs = self._load_docs(verbose)

    def get_context_docs(self, doc_names: Optional[list[str]] = None) -> Dict[str, str]:
        """
        Get requested files.

        Args:
            doc_names: Optional list of document names to retrieve. If None, all documents are returned.

        Returns:
            Dictionary of document name to content.
        """
        if doc_names:
            return {
                doc_name: self.context_docs[doc_name]
                for doc_name in doc_names
                if doc_name in self.context_docs
            }
        return self.context_docs

    def _load_docs(self, verbose: bool) -> Dict[str, str]:
        """Load all documents."""
        docs = {
            os.path.splitext(doc_name)[0]: self._load_doc(doc_name)
            for doc_name in os.listdir(self.docs_dir)
            if self._load_doc(doc_name)
        }

        if verbose:
            print(f"\nLoaded {len(docs)} documents from {self.docs_dir}")

        return docs

    def _load_doc(self, doc_name: str) -> str:
        """Load a document."""
        path = os.path.join(self.docs_dir, doc_name)
        if os.path.exists(path):
            with open(path) as f:
                return f.read()
        return ""
